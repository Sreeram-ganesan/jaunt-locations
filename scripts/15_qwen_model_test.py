from transformers import AutoModelForCausalLM, AutoTokenizer
import torch

# Load the tokenizer and model
# model_name = "Qwen/Qwen-32B"  # Replace with the correct model name if different
# tokenizer = AutoTokenizer.from_pretrained(model_name)
# model = AutoModelForCausalLM.from_pretrained(model_name)
tokenizer = AutoTokenizer.from_pretrained("Qwen/Qwen-7B", trust_remote_code=True)
model = AutoModelForCausalLM.from_pretrained("Qwen/Qwen-7B", device_map="cpu", trust_remote_code=True).eval()

# Ensure the model is on the GPU if available
# device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
# model.to(device)
def generate_text(prompt, input_text, max_length=100):
    # Combine the prompt and input text
    full_prompt = f"{prompt}\n\n{input_text}"
    
    # Tokenize the input prompt
    inputs = tokenizer(full_prompt, return_tensors="pt").to(device)
    
    # Generate text
    outputs = model.generate(
        **inputs,
        max_length=max_length,
        num_return_sequences=1,
        no_repeat_ngram_size=2,
        early_stopping=True
    )
    
    # Decode the generated text
    generated_text = tokenizer.decode(outputs[0], skip_special_tokens=True)
    return generated_text
# Main function
def main():
    text = generate_text("<p>The shape of a triangle, Sheridan Sq isn't much more than a few park benches and some trees surrounded by an old-fashioned wrought-iron gate. But its location (in the heart of gay Greenwich Village) has meant that it has witnessed every rally, demonstration and uprising that has contributed to New York's gay rights movement.</p>", "Improve the following sententce")
if __name__ == "__main__":
    main()