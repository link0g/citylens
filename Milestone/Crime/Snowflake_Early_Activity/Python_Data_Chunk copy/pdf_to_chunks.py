import fitz #Used to open and extract text from PDF files
import os
import pandas as pd
import uuid #Used to generate unique identifiers for each chunk of text extracted from the PDFs

BASE_DIR = os.path.dirname(__file__) 
PDF_FOLDER = os.path.join(BASE_DIR, "../snowflake/Data/Policy")
OUTPUT_FOLDER = BASE_DIR

def get_policy_id(filename):
    if "2021-Boston" in filename:
        return "P001"
    elif "2024-Plan" in filename:
        return "P002"
    elif "2025-The Plan" in filename:
        return "P003"
    elif "S2525" in filename:
        return "P004"
    else:
        return "UNKNOWN"

def chunk_text(text, chunk_size=1000): 
    #Too small → context fragmentation
    #Too large → less similarity precision

    return [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)] #This function splits it into smaller chunks of a specified size (default is 1000 characters) to make it easier to manage
for filename in os.listdir(PDF_FOLDER):
    if filename.endswith(".pdf"):
        
        pdf_path = os.path.join(PDF_FOLDER, filename)
        POLICY_ID = get_policy_id(filename)
        
        print(f"Processing: {filename} → {POLICY_ID}")
        
        doc = fitz.open(pdf_path)
        
        rows = []
        
        for page_number, page in enumerate(doc):
            text = page.get_text()
            chunks = chunk_text(text)
            
            for chunk in chunks:
                rows.append({
                    "POLICY_ID": POLICY_ID,
                    "SOURCE_FILE": filename,
                    "PAGE_NUMBER": page_number + 1,
                    "CHUNK_ID": str(uuid.uuid4()),
                    "CHUNK_TEXT": chunk
                })
        
        df = pd.DataFrame(rows)
        
        output_file = os.path.join(OUTPUT_FOLDER, f"{filename.replace('.pdf','')}_chunks.csv")
        df.to_csv(output_file, index=False)
        
        print(f"Saved: {output_file}")
        print(f"Total chunks: {len(df)}\n")

print("All PDFs processed successfully.")
