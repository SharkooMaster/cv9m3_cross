using System;
using ZstdSharp;

namespace Cross.Services.CcfStore;

/// <summary>
/// Tiny 1-hidden-layer MLP that overfits to a CCF's error value stream.
/// Input: normalized position [0,1], Output: predicted byte value [0,255].
/// Stores int8-quantized weights + sparse residual corrections.
/// </summary>
public static class NeuralErrorCompressor
{
    public const int DefaultHidden = 24;
    public const int MinErrorValues = 32;
    public const int TrainIterations = 2000;

    public static int WeightCount(int h) => 3 * h + 1;

    public sealed class CompressResult
    {
        public byte[] QuantizedWeights = Array.Empty<byte>();
        public float Scale;
        public int Hidden;
        public int ErrorCount;
        public byte[] Residuals = Array.Empty<byte>();
        public int CorrectCount;
    }

    public static CompressResult? TryCompress(byte[] values, int hidden = DefaultHidden)
    {
        if (values.Length < MinErrorValues) return null;

        int n = values.Length;
        int wc = WeightCount(hidden);
        float[] w = new float[wc];

        var rng = new Random(42);
        float s1 = MathF.Sqrt(2f);
        for (int i = 0; i < hidden; i++)
            w[i] = (float)(rng.NextDouble() * 2 - 1) * s1;
        for (int i = hidden; i < 2 * hidden; i++)
            w[i] = 0f;
        float s2 = MathF.Sqrt(2f / hidden);
        for (int i = 2 * hidden; i < 3 * hidden; i++)
            w[i] = (float)(rng.NextDouble() * 2 - 1) * s2;
        w[3 * hidden] = 128f;

        float[] hOut = new float[hidden];
        float[] grad = new float[wc];
        float invN = 1f / n;
        float nMinus1Inv = n > 1 ? 1f / (n - 1) : 0f;

        for (int iter = 0; iter < TrainIterations; iter++)
        {
            Array.Clear(grad);
            float lr = iter < TrainIterations / 2 ? 0.01f : 0.003f;

            for (int i = 0; i < n; i++)
            {
                float x = i * nMinus1Inv;
                if (n == 1) x = 0.5f;

                for (int h = 0; h < hidden; h++)
                {
                    float z = w[h] * x + w[hidden + h];
                    hOut[h] = z > 0 ? z : 0;
                }

                float output = w[3 * hidden];
                for (int h = 0; h < hidden; h++)
                    output += w[2 * hidden + h] * hOut[h];

                float err = output - values[i];

                grad[3 * hidden] += err;
                for (int h = 0; h < hidden; h++)
                    grad[2 * hidden + h] += err * hOut[h];

                for (int h = 0; h < hidden; h++)
                {
                    if (hOut[h] <= 0) continue;
                    float dH = err * w[2 * hidden + h];
                    grad[h] += dH * x;
                    grad[hidden + h] += dH;
                }
            }

            float step = lr * invN;
            for (int j = 0; j < wc; j++)
                w[j] -= grad[j] * step;
        }

        float maxAbs = 0;
        for (int i = 0; i < wc; i++)
            maxAbs = Math.Max(maxAbs, Math.Abs(w[i]));
        float scale = maxAbs > 0 ? 127f / maxAbs : 1f;

        byte[] qw = new byte[wc];
        for (int i = 0; i < wc; i++)
            qw[i] = (byte)(sbyte)Math.Clamp((int)MathF.Round(w[i] * scale), -128, 127);

        float invScale = 1f / scale;
        float[] dqw = new float[wc];
        for (int i = 0; i < wc; i++)
            dqw[i] = (sbyte)qw[i] * invScale;

        byte[] residuals = new byte[n];
        int correct = 0;
        for (int i = 0; i < n; i++)
        {
            byte pred = PredictSingle(dqw, hidden, i, n);
            residuals[i] = (byte)((values[i] - pred) & 0xFF);
            if (residuals[i] == 0) correct++;
        }

        if (correct < n / 5) return null;

        return new CompressResult
        {
            QuantizedWeights = qw,
            Scale = scale,
            Hidden = hidden,
            ErrorCount = n,
            Residuals = residuals,
            CorrectCount = correct
        };
    }

    public static byte[] Decompress(byte[] quantizedWeights, float scale, byte[] residuals, int errorCount, int hidden)
    {
        float invScale = 1f / scale;
        float[] w = new float[quantizedWeights.Length];
        for (int i = 0; i < w.Length; i++)
            w[i] = (sbyte)quantizedWeights[i] * invScale;

        byte[] result = new byte[errorCount];
        for (int i = 0; i < errorCount; i++)
        {
            byte pred = PredictSingle(w, hidden, i, errorCount);
            result[i] = (byte)((residuals[i] + pred) & 0xFF);
        }
        return result;
    }

    private static byte PredictSingle(float[] w, int hidden, int index, int count)
    {
        float x = count > 1 ? (float)index / (count - 1) : 0.5f;
        float output = w[3 * hidden];
        for (int h = 0; h < hidden; h++)
        {
            float z = w[h] * x + w[hidden + h];
            float a = z > 0 ? z : 0;
            output += w[2 * hidden + h] * a;
        }
        return (byte)Math.Clamp((int)MathF.Round(output), 0, 255);
    }

    // ── CCF-level transform: replace error values with residuals in-place ──

    /// <summary>
    /// Parse a v5.6.0 CCF, decompress error values, train a neural net,
    /// replace values with residuals, re-compress, rebuild CCF.
    /// Returns (newCcf, model) if it saved space, null otherwise.
    /// </summary>
    public static (byte[] NewCcf, CompressResult Model)? TransformCcf(byte[] ccf, byte[]? zstdDict, int hidden = DefaultHidden)
    {
        if (ccf.Length < 30) return null;

        var loc = LocateErrorValues(ccf);
        if (loc == null) return null;
        var (valBlobStart, valBlobLen, headerFieldsEnd, refsLen, errorPayloadStart, compModeLen, compBitmaskLen) = loc.Value;

        byte[] rawValues;
        try
        {
            using var d = new Decompressor();
            if (zstdDict != null) d.LoadDictionary(zstdDict);
            rawValues = d.Unwrap(ccf.AsSpan(valBlobStart, valBlobLen)).ToArray();
        }
        catch
        {
            try
            {
                using var d2 = new Decompressor();
                rawValues = d2.Unwrap(ccf.AsSpan(valBlobStart, valBlobLen)).ToArray();
            }
            catch { return null; }
        }

        if (rawValues.Length < MinErrorValues) return null;

        var model = TryCompress(rawValues, hidden);
        if (model == null) return null;

        byte[] newValBlob;
        try
        {
            using var c = new Compressor(3);
            if (zstdDict != null) c.LoadDictionary(zstdDict);
            newValBlob = c.Wrap(model.Residuals).ToArray();
        }
        catch
        {
            using var c2 = new Compressor(3);
            newValBlob = c2.Wrap(model.Residuals).ToArray();
        }

        if (newValBlob.Length >= valBlobLen) return null;

        int sizeDiff = valBlobLen - newValBlob.Length;
        byte[] result = new byte[ccf.Length - sizeDiff];

        Buffer.BlockCopy(ccf, 0, result, 0, valBlobStart);
        Buffer.BlockCopy(newValBlob, 0, result, valBlobStart, newValBlob.Length);
        int afterOldBlob = valBlobStart + valBlobLen;
        if (afterOldBlob < ccf.Length)
            Buffer.BlockCopy(ccf, afterOldBlob, result, valBlobStart + newValBlob.Length, ccf.Length - afterOldBlob);

        int compValLenField = errorPayloadStart + 20;
        BitConverter.TryWriteBytes(result.AsSpan(compValLenField), newValBlob.Length);

        int newErrorCompLen = 24 + compModeLen + compBitmaskLen + newValBlob.Length;
        int errorCompLenFieldPos = headerFieldsEnd;
        BitConverter.TryWriteBytes(result.AsSpan(errorCompLenFieldPos), newErrorCompLen);

        return (result, model);
    }

    /// <summary>
    /// Reverse of TransformCcf: restore original error values from residuals + neural model.
    /// </summary>
    public static byte[] RestoreCcf(byte[] ccf, CompressResult model, byte[]? zstdDict)
    {
        var loc = LocateErrorValues(ccf);
        if (loc == null) return ccf;
        var (valBlobStart, valBlobLen, headerFieldsEnd, refsLen, errorPayloadStart, compModeLen, compBitmaskLen) = loc.Value;

        byte[] residuals;
        try
        {
            using var d = new Decompressor();
            if (zstdDict != null) d.LoadDictionary(zstdDict);
            residuals = d.Unwrap(ccf.AsSpan(valBlobStart, valBlobLen)).ToArray();
        }
        catch
        {
            try
            {
                using var d2 = new Decompressor();
                residuals = d2.Unwrap(ccf.AsSpan(valBlobStart, valBlobLen)).ToArray();
            }
            catch { return ccf; }
        }

        byte[] originalValues = Decompress(
            model.QuantizedWeights, model.Scale, residuals, model.ErrorCount, model.Hidden);

        byte[] restoredBlob;
        try
        {
            using var c = new Compressor(3);
            if (zstdDict != null) c.LoadDictionary(zstdDict);
            restoredBlob = c.Wrap(originalValues).ToArray();
        }
        catch
        {
            using var c2 = new Compressor(3);
            restoredBlob = c2.Wrap(originalValues).ToArray();
        }

        int sizeDiff = restoredBlob.Length - valBlobLen;
        byte[] result = new byte[ccf.Length + sizeDiff];

        Buffer.BlockCopy(ccf, 0, result, 0, valBlobStart);
        Buffer.BlockCopy(restoredBlob, 0, result, valBlobStart, restoredBlob.Length);
        int afterOldBlob = valBlobStart + valBlobLen;
        if (afterOldBlob < ccf.Length)
            Buffer.BlockCopy(ccf, afterOldBlob, result, valBlobStart + restoredBlob.Length, ccf.Length - afterOldBlob);

        int compValLenField = errorPayloadStart + 20;
        BitConverter.TryWriteBytes(result.AsSpan(compValLenField), restoredBlob.Length);

        int newErrorCompLen = 24 + compModeLen + compBitmaskLen + restoredBlob.Length;
        BitConverter.TryWriteBytes(result.AsSpan(headerFieldsEnd), newErrorCompLen);

        return result;
    }

    // ── Serialization for pack header storage ──

    public static byte[] SerializeModel(CompressResult m)
    {
        int len = 1 + 4 + 4 + m.QuantizedWeights.Length;
        byte[] buf = new byte[len];
        buf[0] = (byte)m.Hidden;
        BitConverter.TryWriteBytes(buf.AsSpan(1), m.Scale);
        BitConverter.TryWriteBytes(buf.AsSpan(5), m.ErrorCount);
        Buffer.BlockCopy(m.QuantizedWeights, 0, buf, 9, m.QuantizedWeights.Length);
        return buf;
    }

    public static CompressResult? DeserializeModel(byte[] buf)
    {
        if (buf.Length < 9) return null;
        int hidden = buf[0];
        float scale = BitConverter.ToSingle(buf, 1);
        int errorCount = BitConverter.ToInt32(buf, 5);
        int wc = WeightCount(hidden);
        if (buf.Length < 9 + wc) return null;
        byte[] qw = new byte[wc];
        Buffer.BlockCopy(buf, 9, qw, 0, wc);
        return new CompressResult
        {
            Hidden = hidden,
            Scale = scale,
            ErrorCount = errorCount,
            QuantizedWeights = qw
        };
    }

    // ── Internal: locate the compressed-values blob within a v5.6.0 CCF ──

    private static (int ValBlobStart, int ValBlobLen, int HeaderFieldsEnd, int RefsLen,
        int ErrorPayloadStart, int CompModeLen, int CompBitmaskLen)?
        LocateErrorValues(byte[] ccf)
    {
        try
        {
            int pos = 0;
            int versionLen = BitConverter.ToInt32(ccf, pos); pos += 4;
            if (versionLen <= 0 || versionLen > 20 || pos + versionLen > ccf.Length) return null;
            string version = System.Text.Encoding.UTF8.GetString(ccf, pos, versionLen); pos += versionLen;

            if (version != "v5.6.0") return null;

            int refsLen = BitConverter.ToInt32(ccf, pos); pos += 4;
            int errorCompLenFieldPos = pos;
            int errorCompLen = BitConverter.ToInt32(ccf, pos); pos += 4;
            pos += 4; // errorOrigLen
            pos += 4; // trimLen

            int errorPayloadStart = pos + refsLen;
            if (errorPayloadStart + 24 > ccf.Length) return null;

            int compModeLen = BitConverter.ToInt32(ccf, errorPayloadStart + 12);
            int compBitmaskLen = BitConverter.ToInt32(ccf, errorPayloadStart + 16);
            int compValLen = BitConverter.ToInt32(ccf, errorPayloadStart + 20);

            int valBlobStart = errorPayloadStart + 24 + compModeLen + compBitmaskLen;
            if (valBlobStart + compValLen > ccf.Length) return null;

            return (valBlobStart, compValLen, errorCompLenFieldPos, refsLen,
                errorPayloadStart, compModeLen, compBitmaskLen);
        }
        catch { return null; }
    }
}
