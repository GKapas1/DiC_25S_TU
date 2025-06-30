@echo off
echo ===============================
echo Running full review pipeline...
echo ===============================

REM Optional: activate virtual environment
call .venv\Scripts\activate

echo.
echo [1/3] Setting up resources...
python setup_resources.py

echo.
echo [2/3] Uploading reviews to S3...
python run_testset.py

echo.
echo [3/3] Generating results...
python generate_results.py

echo.
echo ===============================
echo Pipeline completed.
echo ===============================
pause
