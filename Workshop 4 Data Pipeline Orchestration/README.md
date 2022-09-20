## Workshop Data Pipeline Orchestration

**สิ่งที่เรียนรู้จาก Workshop Data Pipeline Orchestration**

1. วิธีการจัดการ Data Pipeline ให้ข้อมูลไหลตามขั้นตอน
   - วิธีเขียน Code Python แต่ละส่วน เช่น วิธีสร้าง DAG 5 ขั้นตอน, สร้าง Task ว่าให้ทำงานส่วนไหนบ้างและส่ง argument ไปยัง Fucntion
2. Cloud Composer 
   - สร้าง Composer สำหรับใช้งาน Airflow บน Google Cloud
   - เข้าไปจัดการ environment บน Composer เช่น ติดตั้ง python package 
3. วิธีการทำ ETL ใน DAG นำ Code Python จาก Workshop 1 Data Collection ใส่ใน function 
4. Airflow 
   - เชื่อมต่อกับ MySQL Database สำหรับดึงข้อมูล
   - วิธีสร้าง DAG 5 ขั้นตอน
   - Operator ที่ใช้ใน Task ใน Workshop ใช้ PythonOperator และ BashOperator
   - Schedule Interval ตั้งเวลาให้ Task รัน Job ใน Workshop นี้ตั้งเวลาเป็น None จะไม่ทำงานอัตโนมัติ
   - การใช้งาน Airflow Web UI เช่น DAGs แสดงการทำงาน Job ทั้งหมดที่รัน, เข้าไปดูการ DAG แสดงรายละเอียดมากขึ้นเช่น Tree View, Graph View, Code
   - การดู Status การทำงานของ Job ติดขัดตรงไหนบ้าง