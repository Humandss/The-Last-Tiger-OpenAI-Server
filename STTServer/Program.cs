using System;
using System.Threading;
using System.Threading.Tasks;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using WebSocketSharp;
using WebSocketSharp.Server;

public static class Program
{
    private static readonly string OpenAIKey = Environment.GetEnvironmentVariable("OPENAI_API_KEY");
    private static readonly Uri RealtimeUri =
        new Uri("wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2025-06-03");

    private static ClientWebSocket _realtime;
    private static readonly StringBuilder _modelText = new StringBuilder();
    private static int inFlight = 0;
    // 각 요청별 고유 번호. inFlight 타임아웃 타이머가 자기 요청만 리셋하도록 보장.
    private static int _requestGen = 0;

    public static async Task Main()
    {
        if (string.IsNullOrWhiteSpace(OpenAIKey))
        {
            Console.WriteLine("OPENAI_API_KEY env var not set.");
            return;
        }

        var wssv = new WebSocketServer(IPAddress.Any, 8787);
        wssv.AddWebSocketService<UnityHub>("/unity");
        wssv.Start();
        Console.WriteLine("Unity WS server: ws://localhost:8787/unity");

        await ConnectRealtime();

        Console.WriteLine("Ready. Press Enter to quit.");
        Console.ReadLine();

        wssv.Stop();
        _realtime?.Abort();
    }

    private static async Task ConnectRealtime()
    {
        _realtime = new ClientWebSocket();
        _realtime.Options.SetRequestHeader("Authorization", $"Bearer {OpenAIKey}");
        _realtime.Options.SetRequestHeader("OpenAI-Beta", "realtime=v1");

        await _realtime.ConnectAsync(RealtimeUri, CancellationToken.None);
        Console.WriteLine("Connected to OpenAI Realtime.");

        await SendRealtime(new
        {
            type = "session.update",
            session = new
            {
                modalities = new[] { "text" },
                instructions = BuildInstructions(),
                turn_detection = (object)null,
                input_audio_format = "pcm16",

                // Whisper prompt: 게임 전용 단어 힌트 → 포수/후진 혼동 등 개선
                input_audio_transcription = new
                {
                    model = "whisper-1",
                    prompt = "포수, 조종수, 장전수, 거너, 로더, 드라이버, " +
                             "전진, 후진, 정지, 멈춰, 좌회전, 우회전, 제자리, 피벗, " +
                             "사격, 발사, 격발, 조준, 에임, 추적, 락온, 취소, 중지, 정렬, 원위치, " +
                             "철갑탄, 고폭탄, 장전, 리로드, " +
                             "사거리, 거리, 레인지, 전속력, 천천히, " +
                             // Whisper가 아라비아 숫자로 출력하도록 유도
                             "사거리 100, 사거리 200, 사거리 300, 사거리 500, " +
                             "사거리 600, 사거리 800, 사거리 1000, 사거리 1200, 사거리 1500, 사거리 2000, " +
                             // 조준/후진 혼동 방지: 조준을 명시적으로 강조
                             "포수 조준, 조준, 에임, 겨냥, 포수 사격, 포수 발사"
                }
            }
        });

        _ = Task.Run(RealtimeReceiveLoop);
        await Task.Delay(100);
        Console.WriteLine("Session configured.");
    }

    public static async Task PushAudioFrame(string base64Pcm16)
    {
        if (_realtime?.State != System.Net.WebSockets.WebSocketState.Open) return;
        await SendRealtime(new { type = "input_audio_buffer.append", audio = base64Pcm16 });
    }

    public static async Task CommitAndRequest()
    {
        if (_realtime?.State != System.Net.WebSockets.WebSocketState.Open) return;
        if (Interlocked.CompareExchange(ref inFlight, 1, 0) == 1)
        {
            Console.WriteLine("[CommitAndRequest] skipped (in-flight)");
            return;
        }

        var gen = Interlocked.Increment(ref _requestGen);

        try
        {
            await SendRealtime(new { type = "input_audio_buffer.commit" });
            await SendRealtime(new
            {
                type = "response.create",
                response = new
                {
                    modalities = new[] { "text" },
                    max_output_tokens = 400,
                    // response.create에는 instructions 넣지 않음
                    //    session.update의 상세 instructions를 그대로 사용
                    //    (여기서 짧은 instructions 넣으면 session 규칙을 덮어써서 오동작)
                }
            });
            Console.WriteLine($"[CommitAndRequest] OK (gen={gen})");
            // 안전장치: 10초 후에도 response.done이 안 오면 자동 해제
            //   gen 비교로 그 사이 시작된 새 요청의 inFlight를 죽이지 않도록 보장
            _ = Task.Delay(10000).ContinueWith(_ =>
            {
                if (Volatile.Read(ref _requestGen) != gen) return;
                if (Interlocked.CompareExchange(ref inFlight, 0, 1) == 1)
                    Console.WriteLine($"[CommitAndRequest] inFlight timeout reset (gen={gen})");
            });
        }
        catch (Exception e)
        {
            Console.WriteLine($"[CommitAndRequest ERROR] {e.Message}");
            Interlocked.Exchange(ref inFlight, 0);
        }
    }

    private static async Task RealtimeReceiveLoop()
    {
        var buf = new byte[64 * 1024];
        var sb = new StringBuilder();

        while (_realtime.State == System.Net.WebSockets.WebSocketState.Open)
        {
            sb.Clear();
            System.Net.WebSockets.WebSocketReceiveResult res;

            try
            {
                do
                {
                    res = await _realtime.ReceiveAsync(buf, CancellationToken.None);
                    if (res.MessageType == System.Net.WebSockets.WebSocketMessageType.Close)
                    {
                        Console.WriteLine("Realtime closed.");
                        NotifyUnityRealtimeDown("openai_realtime_closed");
                        return;
                    }
                    sb.Append(Encoding.UTF8.GetString(buf, 0, res.Count));
                }
                while (!res.EndOfMessage);
            }
            catch (Exception e)
            {
                Console.WriteLine($"[RECEIVE ERROR] {e.Message}");
                break;
            }

            try
            {
                using var doc = JsonDocument.Parse(sb.ToString());
                if (!doc.RootElement.TryGetProperty("type", out var typeEl)) continue;
                var type = typeEl.GetString();

                if (ShouldLog(type))
                    Console.WriteLine($"[RT] {type}");

                switch (type)
                {
                    case "response.text.delta":
                        if (doc.RootElement.TryGetProperty("delta", out var d)
                            && d.ValueKind == JsonValueKind.String)
                        {
                            var delta = d.GetString();
                            if (!string.IsNullOrEmpty(delta))
                                AppendModelText(delta);
                        }
                        break;

                    case "response.done":
                        string status = "unknown";
                        try
                        {
                            var resp = doc.RootElement.GetProperty("response");
                            status = resp.GetProperty("status").GetString() ?? "unknown";

                            if (status != "completed" &&
                                resp.TryGetProperty("status_details", out var details))
                                Console.WriteLine($"[RT DONE] status_details={details}");

                            if (resp.TryGetProperty("usage", out var usage))
                                Console.WriteLine($"[RT DONE] usage={usage}");
                        }
                        catch { }

                        Console.WriteLine($"[RT DONE] status={status}");
                        if (status == "completed") FlushModelTextAsJson();
                        else ClearModelText();
                        Interlocked.Exchange(ref inFlight, 0);
                        break;

                    case "error":
                        Console.WriteLine($"[RT ERROR] {sb}");
                        Interlocked.Exchange(ref inFlight, 0);
                        ClearModelText();
                        break;

                    // STT 결과 로그 - AI가 실제로 뭘 들었는지 확인용
                    case "conversation.item.input_audio_transcription.completed":
                        try
                        {
                            var transcript = doc.RootElement.GetProperty("transcript").GetString();
                            Console.WriteLine($"[STT] \"{transcript}\"");
                        }
                        catch { }
                        break;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"[RT] parse error: {e.Message}");
            }
        }

        Console.WriteLine("[RT] receive loop ended.");
        NotifyUnityRealtimeDown("openai_realtime_disconnected");
    }

    // OpenAI Realtime 연결이 끊겼을 때 Unity 클라이언트에 알림.
    // Unity는 type="server_status", connected=false 를 받으면
    // 음성 명령이 더 이상 처리되지 않음을 UI로 표시해야 함.
    private static void NotifyUnityRealtimeDown(string reason)
    {
        var msg = JsonSerializer.Serialize(new { type = "server_status", connected = false, reason });
        UnityHub.BroadcastToAll(msg);
        Console.WriteLine($"[NOTIFY UNITY] server_status connected=false reason={reason}");
    }

    // =========================================================
    //   한글 숫자 → 아라비아 숫자 변환
    //    Whisper가 "팔백"처럼 한글로 받아쓴 경우를 800으로 변환
    //    FlushModelTextAsJson에서 JSON 추출 전에 raw에 적용
    // =========================================================
    private static string ConvertKoreanNumbers(string text)
    {
        // 만 단위
        var manMap = new (string k, int v)[]
        {
            ("이만", 20000), ("삼만", 30000), ("사만", 40000), ("오만", 50000),
            ("육만", 60000), ("칠만", 70000), ("팔만", 80000), ("구만", 90000), ("만", 10000),
        };
        // 천 단위
        var cheonMap = new (string k, int v)[]
        {
            ("이천", 2000), ("삼천", 3000), ("사천", 4000), ("오천", 5000),
            ("육천", 6000), ("칠천", 7000), ("팔천", 8000), ("구천", 9000), ("천", 1000),
        };
        // 백 단위
        var baekMap = new (string k, int v)[]
        {
            ("이백", 200), ("삼백", 300), ("사백", 400), ("오백", 500),
            ("육백", 600), ("칠백", 700), ("팔백", 800), ("구백", 900), ("백", 100),
        };
        // 십 단위
        var sipMap = new (string k, int v)[]
        {
            ("열", 10), ("이십", 20), ("삼십", 30), ("사십", 40), ("오십", 50),
            ("육십", 60), ("칠십", 70), ("팔십", 80), ("구십", 90),
        };
        // 일 단위
        var ilMap = new (string k, int v)[]
        {
            ("일", 1), ("이", 2), ("삼", 3), ("사", 4), ("오", 5),
            ("육", 6), ("칠", 7), ("팔", 8), ("구", 9),
        };

        // 숫자 앞에 오는 단위 접두어 목록
        var prefixes = new[] { "사거리", "거리", "레인지" };

        foreach (var prefix in prefixes)
        {
            int idx = 0;
            while (true)
            {
                int pos = text.IndexOf(prefix, idx, StringComparison.OrdinalIgnoreCase);
                if (pos < 0) break;

                int start = pos + prefix.Length;
                // 공백 스킵
                while (start < text.Length && text[start] == ' ') start++;

                // 이미 아라비아 숫자면 스킵
                if (start < text.Length && char.IsDigit(text[start])) { idx = start; continue; }

                // 한글 숫자 파싱 시도
                int num = 0, parsed = 0;
                int cur = start;

                foreach (var (k, v) in manMap)
                    if (cur + k.Length <= text.Length && text.Substring(cur, k.Length) == k)
                    { num += v; cur += k.Length; parsed += k.Length; break; }

                foreach (var (k, v) in cheonMap)
                    if (cur + k.Length <= text.Length && text.Substring(cur, k.Length) == k)
                    { num += v; cur += k.Length; parsed += k.Length; break; }

                foreach (var (k, v) in baekMap)
                    if (cur + k.Length <= text.Length && text.Substring(cur, k.Length) == k)
                    { num += v; cur += k.Length; parsed += k.Length; break; }

                foreach (var (k, v) in sipMap)
                    if (cur + k.Length <= text.Length && text.Substring(cur, k.Length) == k)
                    { num += v; cur += k.Length; parsed += k.Length; break; }

                foreach (var (k, v) in ilMap)
                    if (cur + k.Length <= text.Length && text.Substring(cur, k.Length) == k)
                    { num += v; cur += k.Length; parsed += k.Length; break; }

                if (num > 0)
                    text = text.Substring(0, start) + num.ToString() + text.Substring(start + parsed);

                idx = pos + prefix.Length;
            }
        }

        return text;
    }

    // =========================================================
    //   STT 오인식 교정 테이블
    //    Whisper가 자주 헷갈리는 단어 쌍을 강제 교정
    //    FlushModelTextAsJson에서 ConvertKoreanNumbers 다음에 호출
    // =========================================================
    private static string CorrectSttErrors(string text)
    {
        // 긴 패턴 먼저 (짧은 패턴보다 우선)
        var corrections = new (string wrong, string correct)[]
        {
            // "조준" → "후진" 오인식 패턴
            ("포수 후진",   "포수 조준"),
            ("거너 후진",   "포수 조준"),
            ("조종수 후진", "포수 조준"),
            ("운전수 후진", "포수 조준"),
            ("드라이버 후진", "포수 조준"),

            // 단독 "후진" → driver 없으므로 "조준"으로 교정
            // (단, 위 패턴에 안 걸린 나머지)
            ("후진", "조준"),
        };

        foreach (var (wrong, correct) in corrections)
            text = text.Replace(wrong, correct, StringComparison.OrdinalIgnoreCase);

        return text;
    }

    private static bool ShouldLog(string type) =>
        type != "response.text.delta" &&
        type != "input_audio_buffer.speech_started" &&
        type != "input_audio_buffer.speech_stopped";

    public static void AppendModelText(string delta) { lock (_modelText) _modelText.Append(delta); }
    public static void ClearModelText() { lock (_modelText) _modelText.Clear(); }

    public static void FlushModelTextAsJson()
    {
        string raw;
        lock (_modelText) { raw = _modelText.ToString(); _modelText.Clear(); }

        Console.WriteLine($"[FLUSH] raw={raw}");
        if (string.IsNullOrWhiteSpace(raw)) return;

        // 한글 숫자 → 아라비아 숫자 변환 (Whisper가 "팔백" 등으로 인식한 경우 대비)
        raw = ConvertKoreanNumbers(raw);
        // STT 오인식 교정 (후진→조준 등)
        raw = CorrectSttErrors(raw);
        Console.WriteLine($"[STT CORRECTED] {raw}");

        var trimmed = raw.Trim();
        int a = trimmed.IndexOf('{');
        int b = trimmed.LastIndexOf('}');
        if (a < 0 || b <= a) { Console.WriteLine($"[BAD JSON] no braces: {trimmed}"); return; }

        var json = trimmed.Substring(a, b - a + 1);
        try { JsonDocument.Parse(json); }
        catch (Exception e) { Console.WriteLine($"[BAD JSON] {e.Message} | {json}"); return; }

        var payload = JsonSerializer.Serialize(new { type = "cmdjson", json });
        UnityHub.BroadcastToAll(payload);
        Console.WriteLine($"[TO UNITY] {json}");
    }

    private static async Task SendRealtime(object evt)
    {
        try
        {
            var bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(evt));
            await _realtime.SendAsync(bytes,
                System.Net.WebSockets.WebSocketMessageType.Text, true, CancellationToken.None);
        }
        catch (Exception e) { Console.WriteLine($"[SEND ERROR] {e.Message}"); }
    }

    // =========================================================
    //  Instructions - 역할 추론 로직 대폭 강화
    //   핵심: intent 키워드만 있으면 역할을 명확히 지정하도록
    //         driver 키워드 없이도 gunner/loader intent면 해당 역할로 가게
    // =========================================================
    private static string BuildInstructions() => @"
너는 전차 승무원 음성 명령을 게임 명령(JSON)으로 변환하는 전용 파서다.

[절대 규칙 — 반드시 준수]
1. 반드시 JSON 한 개만 출력한다. 예외 없음.
2. JSON 외 어떤 텍스트도 절대 금지. 마크다운/코드블록/설명/대화/인사/포르투갈어/영어/일본어 등 모두 금지.
3. 입력이 음성 명령이 아니거나 알아들을 수 없어도 반드시 JSON으로만 응답한다.
4. 추측 금지. 애매하면 commands=[].
5. 절대로 대화하지 마라. JSON만 출력하라.

[출력 스키마]
{
  ""raw_text"":   string,
  ""confidence"": number,
  ""commands"": [
    {
      ""target_role"":  ""driver|gunner|loader"",
      ""intent"":       ""...(아래 목록 참조)..."",
      ""intensity"":    ""small|normal|large"",
      ""range_meters"": number,
      ""confidence"":   number,
      ""raw_text"":     string
    }
  ]
}

[필드 규칙]
- range_meters: 숫자 없으면 반드시 -1
- intensity: small=천천히/조금/살짝/약하게, large=빨리/빠르게/크게/강하게/전속력, 나머지=normal
- confidence: 확실=0.7이상, 애매=0.4이하

==============================================
[역할 결정 규칙 — 이 순서대로 판단]

STEP 1. 역할 키워드가 있으면 무조건 그 역할
  driver  키워드: 조종수, 운전수, 드라이버 → driver 명령 없으므로 commands=[]
  gunner  키워드: 포수, 거너
  loader  키워드: 장전수, 로더

STEP 2. 역할 키워드가 없으면 → intent로 역할 결정 (아래 표 참조)
  gunner  intent: fire, cease_action, aim_at, align_hull, track_target, set_range
  loader  intent: load_ap, load_he, load_default

  ★ driver는 음성 명령 없음 → driver intent 절대 생성 금지
  ★ 예시: ""사격"" 한 마디 → intent=fire → gunner
  ★ 예시: ""철갑탄"" 한 마디 → intent=load_ap → loader
  ★ 예시: ""전진"" 같은 이동 명령 → commands=[] (driver 없으므로 무시)

STEP 3. intent도 역할도 불분명하면 → commands=[]
==============================================

[intent 키워드 목록 — 없으면 절대 그 intent 생성 금지]

■ driver (이동/조향) — 키보드 전용. 음성 명령 없음. driver intent 절대 생성 금지.

■ gunner (사격/조준)
  fire          : 사격, 발사, 격발, 쏴, 쏴라, 불
  cease_action  : 취소, 중지, 잠깐, 기다려, 사격 취소, 조준 취소
  aim_at        : 조준, 에임, 맞춰, 겨냥
  align_hull    : 정렬, 원위치, 정면, 리셋
  track_target  : 추적, 락온, 록온, 따라가
  set_range     : (거리|사거리|레인지) + 숫자 → 숫자 없으면 set_range 금지

■ loader (장전)
  load_ap      : 철갑, ap, 철갑탄, 관통
  load_he      : 고폭, he, 고폭탄, 폭발
  load_default : 장전, 리로드, 준비, 계속

[혼동 방지]
- 좌/우 발음 불분명 → commands=[]
- 전진/정지 혼동 → commands=[]
- 발사/발차 불분명 → fire 금지
- 숫자 없이 거리/레인지만 → set_range 금지

[다중 명령 허용]
복합 명령(예: 사거리 800 조준)은 commands 배열에 2개 이상 가능.
단 각 confidence >= 0.6 일 때만 포함.
driver는 음성 명령이 없으므로 복합 명령에서도 driver 부분은 제외하고 나머지만 포함.

[few-shot 예시 — 역할 추론 집중]
입력: 사격
출력: {""raw_text"":""사격"",""confidence"":0.95,""commands"":[{""target_role"":""gunner"",""intent"":""fire"",""intensity"":""normal"",""range_meters"":-1,""confidence"":0.95,""raw_text"":""사격""}]}

입력: 철갑탄
출력: {""raw_text"":""철갑탄"",""confidence"":0.95,""commands"":[{""target_role"":""loader"",""intent"":""load_ap"",""intensity"":""normal"",""range_meters"":-1,""confidence"":0.95,""raw_text"":""철갑탄""}]}

입력: 조준
출력: {""raw_text"":""조준"",""confidence"":0.9,""commands"":[{""target_role"":""gunner"",""intent"":""aim_at"",""intensity"":""normal"",""range_meters"":-1,""confidence"":0.9,""raw_text"":""조준""}]}

입력: 전진
출력: {""raw_text"":""전진"",""confidence"":0.0,""commands"":[]}

입력: 전진 천천히
출력: {""raw_text"":""전진 천천히"",""confidence"":0.0,""commands"":[]}

입력: 포수 사격
출력: {""raw_text"":""포수 사격"",""confidence"":0.95,""commands"":[{""target_role"":""gunner"",""intent"":""fire"",""intensity"":""normal"",""range_meters"":-1,""confidence"":0.95,""raw_text"":""사격""}]}

입력: 조종수 오른쪽
출력: {""raw_text"":""조종수 오른쪽"",""confidence"":0.0,""commands"":[]}

입력: 사거리 800
출력: {""raw_text"":""사거리 800"",""confidence"":0.9,""commands"":[{""target_role"":""gunner"",""intent"":""set_range"",""intensity"":""normal"",""range_meters"":800,""confidence"":0.9,""raw_text"":""사거리 800""}]}

입력: 전진하면서 조준
출력: {""raw_text"":""전진하면서 조준"",""confidence"":0.85,""commands"":[{""target_role"":""gunner"",""intent"":""aim_at"",""intensity"":""normal"",""range_meters"":-1,""confidence"":0.85,""raw_text"":""조준""}]}

입력: 사거리 800 조준
출력: {""raw_text"":""사거리 800 조준"",""confidence"":0.9,""commands"":[{""target_role"":""gunner"",""intent"":""set_range"",""intensity"":""normal"",""range_meters"":800,""confidence"":0.9,""raw_text"":""사거리 800""},{""target_role"":""gunner"",""intent"":""aim_at"",""intensity"":""normal"",""range_meters"":-1,""confidence"":0.9,""raw_text"":""조준""}]}

입력: 장전수 철갑탄
출력: {""raw_text"":""장전수 철갑탄"",""confidence"":0.95,""commands"":[{""target_role"":""loader"",""intent"":""load_ap"",""intensity"":""normal"",""range_meters"":-1,""confidence"":0.95,""raw_text"":""철갑탄""}]}

";

    public class UnityHub : WebSocketBehavior
    {
        private static Action<string> _broadcastAction;
        private int _audioFrames = 0;

        protected override void OnOpen()
        {
            Console.WriteLine($"[UNITY] Client connected: {ID}");
            _broadcastAction = msg =>
            {
                try { Sessions.Broadcast(msg); }
                catch (Exception e) { Console.WriteLine($"[BROADCAST ERROR] {e.Message}"); }
            };
        }

        protected override void OnClose(CloseEventArgs e)
            => Console.WriteLine($"[UNITY] Client disconnected: {ID}");

        protected override void OnMessage(MessageEventArgs e)
        {
            try
            {
                using var doc = JsonDocument.Parse(e.Data);
                var root = doc.RootElement;
                if (!root.TryGetProperty("type", out var typeEl)) return;
                var type = typeEl.GetString();

                if (type == "audio")
                {
                    var b64 = root.GetProperty("b64").GetString();
                    if (string.IsNullOrEmpty(b64)) return;
                    _audioFrames++;
                    _ = Program.PushAudioFrame(b64);
                }
                else if (type == "commit")
                {
                    if (_audioFrames == 0)
                    {
                        Console.WriteLine("[COMMIT] skipped (no audio frames)");
                        return;
                    }
                    Console.WriteLine($"[COMMIT] frames={_audioFrames}");
                    _audioFrames = 0;
                    Program.ClearModelText();
                    _ = Program.CommitAndRequest();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[UNITY MSG ERROR] {ex.Message}");
            }
        }

        public static void BroadcastToAll(string message)
            => _broadcastAction?.Invoke(message);
    }
}