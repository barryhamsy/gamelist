@echo off
REM Download Steam images for LAST 30 DAYS
REM Useful for weekly batch processing

echo ========================================
echo Steam Image Downloader - LAST 30 DAYS
echo ========================================
echo.

python appid_image_downloader_with_date_filter.py --last-days 60 -j 32

echo.
echo ========================================
echo Download complete!
echo ========================================
pause