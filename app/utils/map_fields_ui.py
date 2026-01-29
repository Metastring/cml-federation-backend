import gradio as gr
import requests

#CENTRAL_FIELD_API = "http://127.0.0.1:8000/federated-fields"

CENTRAL_FIELD_API = "http://192.168.18.12:8000/federated-fields"
SUBMIT_URL = "http://192.168.18.12:8000/submit-mapping"
#SUBMIT_URL = "http://127.0.0.1:8000/submit-mapping"

AGENT_NAME = "kew_db"  # change this as per the agent


def fetch_fields(): 
    try:
        print("⏳ Fetching fields from central server...")
        response = requests.get(CENTRAL_FIELD_API)
        response.raise_for_status()
        fields = response.json()

        if isinstance(fields, list) and all(isinstance(f, str) for f in fields):
            return fields
        else:
            raise ValueError("Expected list of strings")

    except Exception as e:
        print("❌ ERROR:", e)
        return [f"ERROR: {e}"]

fields = fetch_fields()

with gr.Blocks() as demo:
    gr.Markdown("# 🧩 Agent Field Mapping UI")
    gr.Markdown("Map each open field name to your agent-specific column name.")

    mappings = []
    for field_name in fields:
        with gr.Row():
            open_field = gr.Textbox(value=field_name, label="Open Field", interactive=False)
            column_name = gr.Textbox(label="Your Column Name")
            mappings.append({"field_name_open_for_all": open_field, "column_name": column_name})

    submit_btn = gr.Button("✅ Submit & Generate Mapping")
    #output_json = gr.JSON(label="📝 Final Mapping Output")

    output_json = gr.JSON(label="📝 Submitted Mapping + Server Response")

    def generate_output(*args):
        result = []
        for i in range(0, len(args), 2):
            result.append({
                "field_name_open_for_all": args[i],
                "column_name": args[i+1]
            })

        # POST to central server
        try:
            response = requests.post(
                SUBMIT_URL,
                params={"agent_name": AGENT_NAME},
                json=result
            )
            response.raise_for_status()
            server_response = response.json()
        except Exception as e:
            server_response = {"error": str(e)}

        return {
            "submitted_mapping": result,
            "server_response": server_response
        }


    submit_btn.click(
        fn=generate_output,
        inputs=[val for pair in mappings for val in pair.values()],
        outputs=output_json
    )

if __name__ == "__main__":
    demo.launch()
