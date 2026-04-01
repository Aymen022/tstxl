from app import create_app

cxn_app = create_app()

if __name__ == "__main__":
    cxn_app.run(host="0.0.0.0", port=5000)