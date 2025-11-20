import { SSEParserTransform } from "../utils/SSEParser.transform";
import { SSESerializerTransform } from "../utils/SSESerializer.transform";

// In-memory cache to store the last reasoning signature seen
let lastSignature: any = null;

export class Gemini3FixTransformer {
  name = "gemini3-fix";

  // 1. Outgoing Request: Inject the signature if missing
  async transformRequestIn(request: any): Promise<any> {
    if (request.messages) {
      request.messages.forEach((msg: any) => {
        // Identify the assistant message attempting to call a tool
        if (msg.role === 'assistant' && msg.tool_calls && msg.tool_calls.length > 0) {
          
          // Case A: Signature exists but needs ID fix (Index -> Tool ID)
          if (msg.reasoning_details) {
             const details = Array.isArray(msg.reasoning_details) ? msg.reasoning_details : [msg.reasoning_details];
             details.forEach((detail: any) => {
               if ((detail.index === 0 || !detail.id) && !detail.id) {
                 detail.id = msg.tool_calls[0].id;
                 delete detail.index;
               }
             });
          } 
          // Case B: Signature is missing (dropped by Router). Inject it from our cache.
          else if (lastSignature) {
             // console.log("[Gemini3Fix] Injecting cached signature");
             const injected = JSON.parse(JSON.stringify(lastSignature));
             // Bind the signature to the first tool call ID
             injected.id = msg.tool_calls[0].id;
             delete injected.index;
             
             msg.reasoning_details = [injected];
          }
        }
      });
    }
    return request;
  }

  // 2. Incoming Response: Capture the signature
  async transformResponseOut(response: Response): Promise<Response> {
    const contentType = response.headers.get('content-type');
    if (contentType && contentType.includes('text/event-stream') && response.body) {
      const stream = response.body
        .pipeThrough(new SSEParserTransform())
        .pipeThrough(new TransformStream({
          transform(chunk, controller) {
            try {
              // Detect reasoning details in the stream
              if (chunk.data?.choices?.[0]?.delta?.reasoning_details) {
                  const details = chunk.data.choices[0].delta.reasoning_details;
                  details.forEach((detail: any) => {
                      // If we find an encrypted signature, save it to cache
                      if (detail.type === 'reasoning.encrypted') {
                          lastSignature = detail;
                      }
                  });
                  
                  // Optional: Strip ID to try and help Router save it normally
                  chunk.data.choices[0].delta.reasoning_details = details.map((detail: any) => {
                    if (detail.id) {
                      const { id, ...rest } = detail;
                      return { ...rest, index: 0 };
                    }
                    return detail;
                  });
              }
              controller.enqueue(chunk);
            } catch(e) {
               controller.enqueue(chunk);
            }
          }
        }))
        .pipeThrough(new SSESerializerTransform());

      return new Response(stream, {
        status: response.status,
        statusText: response.statusText,
        headers: response.headers,
      });
    }
    return response;
  }
}

// Export for use as a plugin
module.exports = Gemini3FixTransformer;