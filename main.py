from fastapi import FastAPI, File, UploadFile, HTTPException
import pandas as pd
from io import BytesIO
from sqlalchemy import create_engine, text
import os

app = FastAPI()

# Recupera as variáveis de ambiente ou usa valores padrão para testes locais
DB_HOST = os.getenv("DB_HOST", "aws-0-us-west-1.pooler.supabase.com")
DB_PORT = os.getenv("DB_PORT", "6543")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres.komoeffgbltdarjwnrnc")
DB_PASSWORD = os.getenv("DB_PASSWORD", "@wycGF4565")

# Codifica a senha para a URL (substituindo "@" por "%40")
safe_password = DB_PASSWORD.replace("@", "%40")
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def convert_to_text(val):
    """
    Converte valores ausentes (NaN ou NaT) para None; caso contrário, retorna o valor convertido para string.
    """
    if pd.isna(val):
        return None
    return str(val)

@app.post("/upload_excel/")
async def upload_excel(file: UploadFile = File(...)):
    # Verifica se o arquivo possui a extensão de Excel
    if not file.filename.endswith(('.xls', '.xlsx')):
        raise HTTPException(status_code=400, detail="O arquivo precisa ser um Excel (.xls ou .xlsx)")
    
    try:
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao ler o arquivo: {e}")
    
    # Define as colunas obrigatórias (removendo "Nome da Origem" para que ela seja opcional)
    required_columns = {
        "Número ConLicitação", "Código", "Órgão", "Endereço", "Cidade",
        "Estado", "CEP", "Edital", "Site 1", "Site 2", "Processo", "Valor Estimado",
        "Itens", "Situação", "Documento", "Abertura", "Prazo", "Objeto", "Observação",
        "Anexos", "Atualizada em"
    }
    
    if not required_columns.issubset(df.columns):
        raise HTTPException(
            status_code=400,
            detail=f"Colunas obrigatórias não encontradas no arquivo. Colunas lidas: {df.columns.tolist()}"
        )
    
    try:
        with engine.begin() as connection:
            for idx, row in df.iterrows():
                # Se "Nome da Origem" não existir, usa um valor padrão.
                nome_origem_val = convert_to_text(row.get("Nome da Origem")) if "Nome da Origem" in df.columns else "Não informado"
                
                query = text("""
                    INSERT INTO db_acompanhamentos (
                        "Nome da Origem", "Número ConLicitação", "Código", "Órgão", "Endereço",
                        "Cidade", "Estado", "CEP", "Edital", "Site 1", "Site 2", "Processo",
                        "Valor Estimado", "Itens", "Situação", "Documento", "Abertura", "Prazo",
                        "Objeto", "Observação", "Anexos", "Atualizada em"
                    )
                    VALUES (
                        :nome_origem, :numero_conlicitacao, :codigo, :orgao, :endereco,
                        :cidade, :estado, :cep, :edital, :site1, :site2, :processo,
                        :valor_estimado, :itens, :situacao, :documento, :abertura, :prazo,
                        :objeto, :observacao, :anexos, :atualizada_em
                    )
                """)
                connection.execute(query, {
                    "nome_origem": nome_origem_val,
                    "numero_conlicitacao": convert_to_text(row.get("Número ConLicitação")),
                    "codigo": convert_to_text(row.get("Código")),
                    "orgao": convert_to_text(row.get("Órgão")),
                    "endereco": convert_to_text(row.get("Endereço")),
                    "cidade": convert_to_text(row.get("Cidade")),
                    "estado": convert_to_text(row.get("Estado")),
                    "cep": convert_to_text(row.get("CEP")),
                    "edital": convert_to_text(row.get("Edital")),
                    "site1": convert_to_text(row.get("Site 1")),
                    "site2": convert_to_text(row.get("Site 2")),
                    "processo": convert_to_text(row.get("Processo")),
                    "valor_estimado": convert_to_text(row.get("Valor Estimado")),
                    "itens": convert_to_text(row.get("Itens")),
                    "situacao": convert_to_text(row.get("Situação")),
                    "documento": convert_to_text(row.get("Documento")),
                    "abertura": convert_to_text(row.get("Abertura")),
                    "prazo": convert_to_text(row.get("Prazo")),
                    "objeto": convert_to_text(row.get("Objeto")),
                    "observacao": convert_to_text(row.get("Observação")),
                    "anexos": convert_to_text(row.get("Anexos")),
                    "atualizada_em": convert_to_text(row.get("Atualizada em"))
                })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao inserir dados no banco: {e}")
    
    return {"message": "Dados inseridos com sucesso!"}
