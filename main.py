from fastapi import FastAPI, HTTPException
import os

app = FastAPI()

# endpoint to check the server health
@app.get("/health/")
async def health():
    return {"message": "Server is running."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)