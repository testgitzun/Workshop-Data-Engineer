## Workshop Data Collection

**สิ่งที่เรียนรู้จาก Workshop**

การทำ ETL คือดึงข้อมูลแล้วทำการ Transform ส่วน Load Workshop นี้จะยังไม่โหลดเก็บใน Data Lake
1. อ่านข้อมูลจาก MySQL Database และดึงข้อมูลลจาก REST API 
2. ดึงข้อมูลจาก Tables ใน Database MySQL และข้อมูล API มาแสดงผล
3. เรียกใช้งาน cursor และใช้คำสั่ง SQL ในการ Query วิธี Query ข้อมูลโดยใช้ read_sql() ใน Pandas
4. ทำการแปลงข้อมูลจาก Table เป็น Pandas Dataframe เพื่อความง่ายในการอ่านข้อมูล
5. การเชื่อมต่อข้อมูล 2 ชุดเข้าด้วยกัน
6. ลบเครื่องหมายที่ไม่ต้องในคอลัมน์ Price
7. แปลง Type ข้อมูลให้เป็นข้อมูลที่ถูกต้อง เช่น String เป็น Float
8. สร้าง Function สำหรับคำนวนราคาโดยสร้างคอลัมน์ใหม่
9. Save ข้อมูลที่สมบูรณ์ 
10. เรียนรู้คำสั่ง ฟังก์ชันต่างๆ ใน Pandas เช่น query, filter, summary, rename, groupby กับข้อมูลหนังสือ

**ทดสอบ Merge ที่ branch test-merge**