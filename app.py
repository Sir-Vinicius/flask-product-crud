from flask import Flask, request, jsonify
import pandas as pd

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

# Função para carregar o DataFrame globalmente
def load_dataframe():
    return pd.read_csv("dataset.csv")

# Função para salvar o DataFrame globalmente
def save_dataframe(df):
    df.to_csv("dataset.csv", index=False)

@app.route("/csv", methods=["GET"])
def get_products():
    df = load_dataframe()
    return jsonify({"Produtos": df.to_dict(orient="records")}), 200

@app.route("/csv/<int:product_id>", methods=["GET"])
def get_product_by_id(product_id):
    df = load_dataframe()
    product = df.loc[df["Product ID"] == product_id]
    if product.empty:
        return jsonify({"error": "Produto não encontrado."}), 404
    return jsonify(product.to_dict(orient="records")[0]), 200

@app.route("/csv", methods=["POST"])
def add_product():
    try:
        df = load_dataframe()
        novo_produto = request.get_json()
        validate_product(novo_produto, df)
        
        novo_produto_df = pd.DataFrame([novo_produto])
        novo_produto_df = novo_produto_df.reindex(columns=df.columns, fill_value=pd.NA)
        
        df = pd.concat([df, novo_produto_df], ignore_index=True)
        save_dataframe(df)
        return jsonify({"mensagem": "Produto adicionado com sucesso!"}), 201
    except (KeyError, ValueError) as e:
        return jsonify({"error": str(e)}), 400

@app.route("/csv/<int:product_id>", methods=["PUT"])
def update_product(product_id):
    try:
        df = load_dataframe()
        updated_product = request.get_json()
        validate_product(updated_product, df, update=True)
        
        if product_id not in df["Product ID"].values:
            return jsonify({"error": "Produto não encontrado."}), 404
        
        # Atualizar o DataFrame com os novos dados
        df.loc[df["Product ID"] == product_id] = updated_product.values()
        
        # Salvar o DataFrame atualizado no arquivo CSV
        save_dataframe(df)
        
        return jsonify({"mensagem": "Produto atualizado com sucesso!"}), 200
    except (KeyError, ValueError) as e:
        return jsonify({"error": str(e)}), 400

@app.route("/csv/<int:product_id>", methods=["DELETE"])
def delete_product(product_id):
    try:
        df = load_dataframe()
        if product_id not in df["Product ID"].values:
            return jsonify({"error": "Produto não encontrado."}), 404
        
        df = df[df["Product ID"] != product_id]
        save_dataframe(df)
        return jsonify({"mensagem": "Produto excluído com sucesso!"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

def validate_product(product, df, update=False):
    required_fields = [
        "Product ID",
        "Product Title",
        "Merchant ID",
        "Cluster ID",
        "Cluster Label",
        "Category ID",
        "Category Label",
    ]
    for field in required_fields:
        if field not in product:
            raise KeyError(f"Campo obrigatório '{field}' ausente.")
    
    if not update and product["Product ID"] in df["Product ID"].values:
        raise ValueError("ID do produto já existe.")
    
    return True

if __name__ == "__main__":
    app.run(debug=True)
