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
using BepInEx.Logging;
using HarmonyLib;
using UnityEngine;

namespace SmoothSave;

[BepInPlugin(ModGUID, ModName, ModVersion)]
[BepInIncompatibility("org.bepinex.plugins.valheim_plus")]
public class SmoothSave : BaseUnityPlugin
{
	private const string ModName = "Smooth Save";
	private const string ModVersion = "1.0.3";
	private const string ModGUID = "org.bepinex.plugins.smoothsave";

	private static SmoothSave selfReference = null!;
	private static ManualLogSource logger => selfReference.Logger;
	
	private static ConfigEntry<int> zdoBatchSize = null!;
	private static ConfigEntry<Logging> saveLoggingOutput = null!;

	private enum Logging
	{
		Off,
		Simple,
		Detailed,
	}

	public void Awake()
	{
		selfReference = this;

		Assembly assembly = Assembly.GetExecutingAssembly();
		Harmony harmony = new(ModGUID);
		harmony.PatchAll(assembly);

		zdoBatchSize = Config.Bind("1 - General", "ZDOs to copy at once", 3000, "The number of ZDOs that are copied at the same time. You can increase this, if you have powerful hardware.");
		saveLoggingOutput = Config.Bind("1 - General", "World save output", Logging.Simple, "Log level for world saves.");
	}

	static SmoothSave()
	{
		customZDOCopyTask.SetResult(true);
	}

	private static TaskCompletionSource<bool> customZDOCopyTask = new();

	private static readonly List<int> removedIndices = new();
	private static Dictionary<ZDOID, int>? copiedZdoIndices;
	private static List<ZDO> copiedZdos = null!;
	private static Dictionary<ZDOID, BinarySearchDictionary<int, float>> copiedZdo_floats = new();
	private static Dictionary<ZDOID, BinarySearchDictionary<int, Vector3>> copiedZdo_vec3 = new();
	private static Dictionary<ZDOID, BinarySearchDictionary<int, Quaternion>> copiedZdo_quats = new();
	private static Dictionary<ZDOID, BinarySearchDictionary<int, int>> copiedZdo_ints = new();
	private static Dictionary<ZDOID, BinarySearchDictionary<int, long>> copiedZdo_longs = new();
	private static Dictionary<ZDOID, BinarySearchDictionary<int, string>> copiedZdo_strings = new();
	private static Dictionary<ZDOID, BinarySearchDictionary<int, byte[]>> copiedZdo_byteArrays = new();

	private static int copyingSectorId;
	private static int copyingSectorOffset;
	
	private static void Log(string message)
	{
		logger.LogMessage(message);
	}

	[HarmonyPatch(typeof(ZNet), nameof(ZNet.SaveWorldThread))]
	public class SplitSaveWorld
	{
		private static bool Prefix()
		{
			Task<bool> task = customZDOCopyTask.Task;
			task.Wait();
			return task.Result;
		}
	}

	[HarmonyPatch(typeof(ZDO), nameof(ZDO.IncreaseDataRevision))]
	public class TrackZDOChanges
	{
		private static void CloneUpdatedZDO(ZDO zdo)
		{
			if (copiedZdoIndices!.TryGetValue(zdo.m_uid, out int index))
			{
				copiedZdos[index] = zdo.Clone();
				UpdateZDOExtraData(zdo.m_uid);
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
				UpdateZDOExtraData(zdo.m_uid);
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

					instructions.InsertRange(i + 2, new[]
					{
						new CodeInstruction(OpCodes.Ldsfld, AccessTools.DeclaredField(typeof(SmoothSave), nameof(copiedZdoIndices))),
						new CodeInstruction(OpCodes.Brfalse, endLabel),
						new CodeInstruction(OpCodes.Call, AccessTools.DeclaredMethod(typeof(TrackZDOChanges), nameof(CloneUpdatedZDO))),
						new CodeInstruction(OpCodes.Pop) { labels = { endLabel } },
					});

					instructions.Insert(i - 1, new CodeInstruction(OpCodes.Dup));

					i += 6;
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
					ZDOID zdoid = zdo.m_uid;
					removedIndices.Add(copyIndex);
					indices.Remove(zdoid);
					copiedZdo_floats.Remove(zdoid);
					copiedZdo_vec3.Remove(zdoid);
					copiedZdo_quats.Remove(zdoid);
					copiedZdo_ints.Remove(zdoid);
					copiedZdo_longs.Remove(zdoid);
					copiedZdo_strings.Remove(zdoid);
					copiedZdo_byteArrays.Remove(zdoid);
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
			if (copiedZdoIndices != null && index < copyingSectorId && zdo.Persistent)
			{
				copiedZdoIndices.Add(zdo.m_uid, copiedZdos.Count);
				copiedZdos.Add(zdo);
				UpdateZDOExtraData(zdo.m_uid);
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

	private static void UpdateZDOExtraData(ZDOID zdoid)
	{
		if (ZDOExtraData.s_floats.TryGetValue(zdoid, out BinarySearchDictionary<int, float>? value_floats))
		{
			copiedZdo_floats[zdoid] = (BinarySearchDictionary<int, float>)value_floats.Clone();
		}
		if (ZDOExtraData.s_vec3.TryGetValue(zdoid, out BinarySearchDictionary<int, Vector3>? value_vec3))
		{
			copiedZdo_vec3[zdoid] = (BinarySearchDictionary<int, Vector3>)value_vec3.Clone();
		}
		if (ZDOExtraData.s_quats.TryGetValue(zdoid, out BinarySearchDictionary<int, Quaternion>? value_quats))
		{
			copiedZdo_quats[zdoid] = (BinarySearchDictionary<int, Quaternion>)value_quats.Clone();
		}
		if (ZDOExtraData.s_ints.TryGetValue(zdoid, out BinarySearchDictionary<int, int>? value_ints))
		{
			copiedZdo_ints[zdoid] = (BinarySearchDictionary<int, int>)value_ints.Clone();
		}
		if (ZDOExtraData.s_longs.TryGetValue(zdoid, out BinarySearchDictionary<int, long>? value_longs))
		{
			copiedZdo_longs[zdoid] = (BinarySearchDictionary<int, long>)value_longs.Clone();
		}
		if (ZDOExtraData.s_strings.TryGetValue(zdoid, out BinarySearchDictionary<int, string>? value_strings))
		{
			copiedZdo_strings[zdoid] = (BinarySearchDictionary<int, string>)value_strings.Clone();
		}
		if (ZDOExtraData.s_byteArrays.TryGetValue(zdoid, out BinarySearchDictionary<int, byte[]>? value_byteArrays))
		{
			copiedZdo_byteArrays[zdoid] = (BinarySearchDictionary<int, byte[]>)value_byteArrays.Clone();
		}
	}

	private static void AbortSave()
	{
		customZDOCopyTask.SetResult(false);
		Log("Aborted saving due to new save request while ZDOs for saving are being collected.");
	}

	[HarmonyPatch(typeof(ZNet), nameof(ZNet.SaveWorld))]
	public class ReplaceZDOCopying
	{
		private static void CustomZDOCopy(ZDOMan zdoMan)
		{
			TaskCompletionSource<bool> task = new();
			customZDOCopyTask = task;

			IEnumerator save()
			{
				yield return null;

				Dictionary<ZDOID, int> zdoIndex = new();
				List<ZDO> saveZDOs = new();

				Dictionary<ZDOID, BinarySearchDictionary<int, float>> zdo_floats = new();
				Dictionary<ZDOID, BinarySearchDictionary<int, Vector3>> zdo_vec3 = new();
				Dictionary<ZDOID, BinarySearchDictionary<int, Quaternion>> zdo_quats = new();
				Dictionary<ZDOID, BinarySearchDictionary<int, int>> zdo_ints = new();
				Dictionary<ZDOID, BinarySearchDictionary<int, long>> zdo_longs = new();
				Dictionary<ZDOID, BinarySearchDictionary<int, string>> zdo_strings = new();
				Dictionary<ZDOID, BinarySearchDictionary<int, byte[]>> zdo_byteArrays = new();

				zdoMan.m_saveData = new ZDOMan.SaveData
				{
					m_sessionID = zdoMan.m_sessionID,
					m_zdos = saveZDOs,
				};

				yield return null;

				copiedZdos = saveZDOs;
				copiedZdoIndices = zdoIndex;
				copiedZdo_floats = zdo_floats;
				copiedZdo_vec3 = zdo_vec3;
				copiedZdo_quats = zdo_quats;
				copiedZdo_ints = zdo_ints;
				copiedZdo_longs = zdo_longs;
				copiedZdo_strings = zdo_strings;
				copiedZdo_byteArrays = zdo_byteArrays;

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
							if (zdo.Persistent)
							{
								ZDOID zdoid = zdo.m_uid;

								zdoIndex.Add(zdoid, saveZDOs.Count);
								saveZDOs.Add(zdo.Clone());

								if (ZDOExtraData.s_floats.TryGetValue(zdoid, out BinarySearchDictionary<int, float>? value_floats))
								{
									zdo_floats.Add(zdoid, (BinarySearchDictionary<int, float>)value_floats.Clone());
								}
								if (ZDOExtraData.s_vec3.TryGetValue(zdoid, out BinarySearchDictionary<int, Vector3>? value_vec3))
								{
									zdo_vec3.Add(zdoid, (BinarySearchDictionary<int, Vector3>)value_vec3.Clone());
								}
								if (ZDOExtraData.s_quats.TryGetValue(zdoid, out BinarySearchDictionary<int, Quaternion>? value_quats))
								{
									zdo_quats.Add(zdoid, (BinarySearchDictionary<int, Quaternion>)value_quats.Clone());
								}
								if (ZDOExtraData.s_ints.TryGetValue(zdoid, out BinarySearchDictionary<int, int>? value_ints))
								{
									zdo_ints.Add(zdoid, (BinarySearchDictionary<int, int>)value_ints.Clone());
								}
								if (ZDOExtraData.s_longs.TryGetValue(zdoid, out BinarySearchDictionary<int, long>? value_longs))
								{
									zdo_longs.Add(zdoid, (BinarySearchDictionary<int, long>)value_longs.Clone());
								}
								if (ZDOExtraData.s_strings.TryGetValue(zdoid, out BinarySearchDictionary<int, string>? value_strings))
								{
									zdo_strings.Add(zdoid, (BinarySearchDictionary<int, string>)value_strings.Clone());
								}
								if (ZDOExtraData.s_byteArrays.TryGetValue(zdoid, out BinarySearchDictionary<int, byte[]>? value_byteArrays))
								{
									zdo_byteArrays.Add(zdoid, (BinarySearchDictionary<int, byte[]>)value_byteArrays.Clone());
								}

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
						if (zdo.Persistent)
						{
							saveZDOs.Add(zdo.Clone());
						}
					}
				}

				stopwatch.Stop();

				zdoMan.m_saveData.m_nextUid = zdoMan.m_nextUid;

				ZDOExtraData.s_saveFloats = zdo_floats;
				ZDOExtraData.s_saveVec3s = zdo_vec3;
				ZDOExtraData.s_saveQuats = zdo_quats;
				ZDOExtraData.s_saveInts = zdo_ints;
				ZDOExtraData.s_saveLongs = zdo_longs;
				ZDOExtraData.s_saveStrings = zdo_strings;
				ZDOExtraData.s_saveByteArrays = zdo_byteArrays;

				long outsideSectorTime = stopwatch.ElapsedMilliseconds - outsideSectorStart;

				long connectionHashDataStart = stopwatch.ElapsedMilliseconds;

				ZDOExtraData.RegenerateConnectionHashData();
				ZDOExtraData.s_saveConnections = ZDOExtraData.s_connectionsHashData.Clone();

				long connectionHashDataTime = stopwatch.ElapsedMilliseconds - connectionHashDataStart;

				if (saveLoggingOutput.Value == Logging.Simple)
				{
					Log($"World saved: Estimated blocking time without the mod: {stopwatch.ElapsedMilliseconds} ms. Blocking time now: {longestBlockingTime} ms.");
				}
				else if (saveLoggingOutput.Value == Logging.Detailed)
				{
					Log($"Copying ZDOs into internal buffer took {stopwatch.ElapsedMilliseconds} ms. (Longest blocking time: {longestBlockingTime} ms, copying ZDOs outside sector time: {outsideSectorTime} ms, saving connections: {connectionHashDataTime} ms).");
				}

				ZoneSystem.instance.PrepareSave();
				RandEventSystem.instance.PrepareSave();

				copiedZdos = null!;
				copiedZdoIndices = null;

				ZNet.instance.m_saveThreadStartTime = Time.realtimeSinceStartup;
				task.SetResult(true);
			}

			IEnumerator saveCoroutine = save();
			ZNet.instance.StartCoroutine(saveCoroutine);

			// On abort
			customZDOCopyTask.Task.ContinueWith(_ => ZNet.instance.StopCoroutine(saveCoroutine));
		}

		// If we are still in process of collecting ZDOs, no point in restarting saving
		public static bool Prefix(bool sync)
		{
			if (customZDOCopyTask.Task.IsCompleted)
			{
				return true;
			}

			if (sync)
			{
				AbortSave();
				return true;
			}

			return false;
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

	[HarmonyPatch(typeof(ZNet), nameof(ZNet.StopAll))]
	private class InterceptSaveThreadJoinOnStop
	{
		public static void Prefix(ZNet __instance)
		{
			if (!__instance.m_haveStoped && !customZDOCopyTask.Task.IsCompleted)
			{
				__instance.SaveWorld(true);
			}
		}
	}
}
