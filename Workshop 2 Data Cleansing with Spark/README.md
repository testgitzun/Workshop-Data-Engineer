## Workshop Data Cleansing with Spark

**สิ่งที่เรียนรู้จาก Workshop**

1. ติดตั้ง Spark และ PySpark
2. โหลด Data จาก Link ที่เป็น .zip และทำการ unzip เป็นไฟล์ csv
3. ใช้ Spark โหลดข้อมูล csv
4. ตรวจสอบข้อมูลด้วย Data Profiling เช่น มีกี่คอลัมน์, Type ของคอลัมน์, นับจำนวนแถวและคอลัมน์
5. Exploratory Data Analysis(EDA) ดูรายละเอียด แบบตัวเลข เช่น ข้อมูลสถิติ และแบบกราฟฟิก เช่น Boxplot, Histogram, Scatterplot, JoinJPlot
6. ทำความสะอาดข้อมูลด้วย Spark คอลัมน์ที่มีค่า null, ข้อความที่สะกดผิด, รหัสที่เกิน 8 หลัก
7. Clean ข้อมูลด้วย Spark SQL
8. เลือกข้อมูลด้วยคำสั่ง select(), when()