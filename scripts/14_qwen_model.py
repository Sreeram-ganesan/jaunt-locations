from transformers import AutoModelForCausalLM, AutoTokenizer
import torch

# Load the tokenizer and model
model_name = "Qwen/Qwen-32B"  # Replace with the correct model name if different
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForCausalLM.from_pretrained(model_name)

# Ensure the model is on the GPU if available
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model.to(device)

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

# Example usage
# prompt = "Summarize the following text:"
# input_text = "Once upon a time, in a faraway land, there was a brave knight named Sir Cedric. He lived in a castle surrounded by dense forests and towering mountains. Sir Cedric was known throughout the kingdom for his valor and chivalry. One day, a mysterious traveler arrived at the castle gates, seeking the knight's assistance on a perilous quest."
# generated_text = generate_text(prompt, input_text)
# print(generated_text)