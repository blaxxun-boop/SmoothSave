using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading.Tasks;
using BepInEx;
using BepInEx.Configuration;
using HarmonyLib;
using UnityEngine;
using Debug = UnityEngine.Debug;

namespace SmoothSave;

[BepInPlugin(ModGUID, ModName, ModVersion)]
public class SmoothSave : BaseUnityPlugin
{
	private const string ModName = "Smooth Save";
	private const string ModVersion = "1.0.0";
	private const string ModGUID = "org.bepinex.plugins.smoothsave";

	private static ConfigEntry<int> zdoBatchSize = null!;
	private static ConfigEntry<Logging> saveLoggingOutput = null!;

	private enum Logging
	{
		Off,
		Simple,
		Detailed
	}

	public void Awake()
	{
		Assembly assembly = Assembly.GetExecutingAssembly();
		Harmony harmony = new(ModGUID);
		harmony.PatchAll(assembly);

		zdoBatchSize = Config.Bind("1 - General", "ZDOs to copy at once", 3000, "The number of ZDOs that are copied at the same time. You can increase this, if you have powerful hardware.");
		saveLoggingOutput = Config.Bind("1 - General", "World save output", Logging.Simple, "Log level for world saves.");
	}

	private static Task customZDOCopyTask = Task.CompletedTask;

	private static Dictionary<ZDOID, int>? copiedZdoIndices;
	private static List<ZDO> copiedZdos = null!;
	private static readonly List<int> removedIndices = new();

	private static int copyingSectorId;
	private static int copyingSectorOffset;

	[HarmonyPatch(typeof(ZNet), nameof(ZNet.SaveWorldThread))]
	public class SplitSaveWorld
	{
		private static void Prefix()
		{
			customZDOCopyTask.Wait();
		}
	}

	[HarmonyPatch(typeof(ZDO), nameof(ZDO.IncreseDataRevision))]
	public class TrackZDOChanges
	{
		private static void CloneUpdatedZDO(ZDO zdo)
		{
			if (copiedZdoIndices!.TryGetValue(zdo.m_uid, out int index))
			{
				copiedZdos[index] = zdo.Clone();
			}
		}

		private static IEnumerable<CodeInstruction> Transpiler(IEnumerable<CodeInstruction> instructions, ILGenerator ilg)
		{
			foreach (CodeInstruction instruction in instructions)
			{
				if (instruction.opcode == OpCodes.Ret)
				{
					Label endLabel = ilg.DefineLabel();

					yield return new CodeInstruction(OpCodes.Ldsfld, AccessTools.DeclaredField(typeof(SmoothSave), nameof(copiedZdoIndices)));
					yield return new CodeInstruction(OpCodes.Brtrue, endLabel);
					yield return new CodeInstruction(OpCodes.Ret);
					yield return new CodeInstruction(OpCodes.Ldarg_0) { labels = { endLabel } };
					yield return new CodeInstruction(OpCodes.Call, AccessTools.DeclaredMethod(typeof(TrackZDOChanges), nameof(CloneUpdatedZDO)));
				}

				yield return instruction;
			}
		}
	}

	[HarmonyPatch(typeof(ZDOMan), nameof(ZDOMan.RPC_ZDOData))]
	public class TrackZDORPCChanges
	{
		private static ZDO CloneUpdatedZDO(ZDO zdo)
		{
			if (copiedZdoIndices!.TryGetValue(zdo.m_uid, out int index))
			{
				copiedZdos[index] = zdo.Clone();
			}

			return zdo;
		}

		private static IEnumerable<CodeInstruction> Transpiler(IEnumerable<CodeInstruction> _instructions, ILGenerator ilg)
		{
			List<CodeInstruction> instructions = _instructions.ToList();
			MethodInfo DeserializeZDO = AccessTools.DeclaredMethod(typeof(ZDO), nameof(ZDO.Deserialize));

			for (int i = 0; i < instructions.Count; ++i)
			{
				if (instructions[i].opcode == OpCodes.Callvirt && instructions[i].OperandIs(DeserializeZDO))
				{

					Label endLabel = ilg.DefineLabel();

					instructions.InsertRange(i + 1, new[]
					{
						new CodeInstruction(OpCodes.Ldsfld, AccessTools.DeclaredField(typeof(SmoothSave), nameof(copiedZdoIndices))),
						new CodeInstruction(OpCodes.Brfalse, endLabel),
						new CodeInstruction(OpCodes.Call, AccessTools.DeclaredMethod(typeof(TrackZDOChanges), nameof(CloneUpdatedZDO))),
						new CodeInstruction(OpCodes.Pop) { labels = { endLabel } },
					});

					instructions.Insert(i - 1, new CodeInstruction(OpCodes.Dup));

					i += 5;
				}
			}

			return instructions;
		}
	}

	[HarmonyPatch(typeof(ZDOMan), nameof(ZDOMan.RemoveFromSector))]
	public class TrackRemovedZDOs
	{
		private static bool RemoveZDO(List<ZDO> zdos, ZDO zdo, int index)
		{
			int zdoIndex = zdos.IndexOf(zdo);
			if (zdoIndex < 0)
			{
				return false;
			}

			zdos.RemoveAt(zdoIndex);
			if (copiedZdoIndices is { } indices)
			{
				if (copyingSectorId == index && zdoIndex <= copyingSectorOffset)
				{
					--copyingSectorOffset;
				}
				else if (copyingSectorId <= index)
				{
					return true;
				}

				// might not be defined for non-persistent objects
				if (indices.TryGetValue(zdo.m_uid, out int copyIndex))
				{
					removedIndices.Add(copyIndex);
					indices.Remove(zdo.m_uid);
				}
			}

			return true;
		}

		private static IEnumerable<CodeInstruction> Transpiler(IEnumerable<CodeInstruction> instructions)
		{
			MethodInfo listRemove = AccessTools.DeclaredMethod(typeof(List<ZDO>), nameof(List<ZDO>.Remove), new[] { typeof(ZDO) });
			bool first = true;
			foreach (CodeInstruction instruction in instructions)
			{
				if (first && instruction.opcode == OpCodes.Callvirt && instruction.OperandIs(listRemove))
				{
					first = false;
					yield return new CodeInstruction(OpCodes.Ldloc_0);
					yield return new CodeInstruction(OpCodes.Call, AccessTools.DeclaredMethod(typeof(TrackRemovedZDOs), nameof(RemoveZDO)));
				}
				else
				{
					yield return instruction;
				}
			}
		}
	}

	[HarmonyPatch(typeof(ZDOMan), nameof(ZDOMan.AddToSector))]
	public class TrackAddedZDOs
	{
		private static void AddZdo(ZDO zdo, int index)
		{
			if (copiedZdoIndices != null && index < copyingSectorId && zdo.m_persistent)
			{
				copiedZdoIndices.Add(zdo.m_uid, copiedZdos.Count);
				copiedZdos.Add(zdo);
			}
		}

		private static IEnumerable<CodeInstruction> Transpiler(IEnumerable<CodeInstruction> instructions)
		{
			MethodInfo listAdd = AccessTools.DeclaredMethod(typeof(List<ZDO>), nameof(List<ZDO>.Add), new[] { typeof(ZDO) });
			int firsts = 2; // .Add() on list init and generic .Add()
			foreach (CodeInstruction instruction in instructions)
			{
				yield return instruction;
				if (firsts > 0 && instruction.opcode == OpCodes.Callvirt && instruction.OperandIs(listAdd))
				{
					--firsts;
					yield return new CodeInstruction(OpCodes.Ldarg_1);
					yield return new CodeInstruction(OpCodes.Ldloc_0);
					yield return new CodeInstruction(OpCodes.Call, AccessTools.DeclaredMethod(typeof(TrackAddedZDOs), nameof(AddZdo)));
				}
			}
		}
	}

	[HarmonyPatch(typeof(ZNet), nameof(ZNet.SaveWorld))]
	public class ReplaceZDOCopying
	{
		private static void CustomZDOCopy(ZDOMan zdoMan)
		{
			IEnumerator save()
			{
				TaskCompletionSource<object?> task = new();
				customZDOCopyTask = task.Task;

				yield return null;

				Dictionary<ZDOID, int> zdoIndex = new();
				List<ZDO> saveZDOs = new();

				zdoMan.m_saveData = new ZDOMan.SaveData
				{
					m_myid = zdoMan.m_myid,
					m_zdos = saveZDOs,
				};

				yield return null;

				copiedZdos = saveZDOs;
				copiedZdoIndices = zdoIndex;

				Stopwatch stopwatch = Stopwatch.StartNew();
				long longestBlockingTime = 0, lastElapsed = 0;
				int currentBatch = zdoBatchSize.Value;

				List<ZDO>?[] objectsBySector = zdoMan.m_objectsBySector;
				for (int index = 0, end = objectsBySector.Length; index < end; ++index)
				{
					if (objectsBySector[index] is { } zdos)
					{
						for (int i = 0; i < zdos.Count; ++i)
						{
							ZDO zdo = zdos[i];
							if (zdo.m_persistent)
							{
								zdoIndex.Add(zdo.m_uid, saveZDOs.Count);
								saveZDOs.Add(zdo.Clone());

								if (--currentBatch <= 0)
								{
									currentBatch = zdoBatchSize.Value;
									copyingSectorId = index;
									copyingSectorOffset = i;

									stopwatch.Stop();
									longestBlockingTime = Math.Max(longestBlockingTime, stopwatch.ElapsedMilliseconds - lastElapsed);
									lastElapsed = stopwatch.ElapsedMilliseconds;
									yield return null;
									stopwatch.Start();

									i = copyingSectorOffset;
								}
							}
						}
					}
				}

				foreach (int removedIndex in removedIndices.OrderByDescending(i => i))
				{
					int lastIndex = saveZDOs.Count - 1;
					saveZDOs[removedIndex] = saveZDOs[lastIndex];
					saveZDOs.RemoveAt(lastIndex);
				}
				removedIndices.Clear();

				long outsideSectorStart = stopwatch.ElapsedMilliseconds;

				foreach (List<ZDO> zdoList in zdoMan.m_objectsByOutsideSector.Values)
				{
					foreach (ZDO zdo in zdoList)
					{
						if (zdo.m_persistent)
						{
							saveZDOs.Add(zdo.Clone());
						}
					}
				}

				stopwatch.Stop();

				Stopwatch deadZdoStopWatch = Stopwatch.StartNew();
				zdoMan.m_saveData.m_deadZDOs = new Dictionary<ZDOID, long>(zdoMan.m_deadZDOs);
				deadZdoStopWatch.Stop();

				zdoMan.m_saveData.m_nextUid = zdoMan.m_nextUid;

				if (saveLoggingOutput.Value == Logging.Simple)
				{
					Debug.Log($"World saved: Estimated blocking time without the mod: {stopwatch.ElapsedMilliseconds} ms. Blocking time now: {longestBlockingTime} ms.");
				}
				else if (saveLoggingOutput.Value == Logging.Detailed)
				{
					Debug.Log($"Copying ZDOs into internal buffer took {stopwatch.ElapsedMilliseconds} ms. (Longest blocking time: {longestBlockingTime} ms, copying ZDOs outside sector time: {stopwatch.ElapsedMilliseconds - outsideSectorStart} ms). Copying dead ZDOs took {deadZdoStopWatch.ElapsedMilliseconds} ms.");
				}

				ZoneSystem.instance.PrepareSave();
				RandEventSystem.instance.PrepareSave();

				ZNet.instance.m_saveThreadStartTime = Time.realtimeSinceStartup;
				task.SetResult(null);

				copiedZdos = null!;
				copiedZdoIndices = null;
			}

			ZNet.instance.StartCoroutine(save());
		}

		public static IEnumerable<CodeInstruction> Transpiler(IEnumerable<CodeInstruction> instructions, ILGenerator ilg)
		{
			MethodInfo PrepareSave = AccessTools.DeclaredMethod(typeof(ZDOMan), nameof(ZDOMan.PrepareSave));
			bool insertCustomSave = false;
			Label doCustomSaveLabel = ilg.DefineLabel();

			foreach (CodeInstruction instruction in instructions)
			{
				if (insertCustomSave)
				{
					insertCustomSave = false;
					Label endCustomSaveLabel = ilg.DefineLabel();
					yield return new CodeInstruction(OpCodes.Br, endCustomSaveLabel);
					yield return new CodeInstruction(OpCodes.Call, AccessTools.DeclaredMethod(typeof(ReplaceZDOCopying), nameof(CustomZDOCopy))) { labels = { doCustomSaveLabel } };
					instruction.labels.Add(endCustomSaveLabel);
				}
				if (instruction.opcode == OpCodes.Callvirt && instruction.OperandIs(PrepareSave))
				{
					yield return new CodeInstruction(OpCodes.Ldarg_1); // sync parameter
					yield return new CodeInstruction(OpCodes.Brfalse, doCustomSaveLabel);
					insertCustomSave = true;
				}
				yield return instruction;
			}
		}
	}
}
