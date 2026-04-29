--CREATE EXTENSION IF NOT EXISTS citext;
drop table if exists int.employees;
drop table if exists stg.clean_hrm_employees;


-- CLEAN STAGE : employees từ file HRM (quá bẩn)
create table stg.clean_hrm_employees as (
with clean_hrm_employees as (
	select * ,

	-- clean ngày sinh
	COALESCE(
        -- Trường hợp 1: Định dạng YYYY-MM-DD 
        CASE 
            WHEN ngay_sinh ~ '^\d{4}-\d{2}-\d{2}' 
            THEN LEFT(ngay_sinh, 10)::DATE 
        END,
        -- Trường hợp 2: Định dạng DD/MM/YYYY
        CASE 
            WHEN ngay_sinh ~ '^\d{2}/\d{2}/\d{4}' 
            THEN TO_DATE(ngay_sinh, 'DD/MM/YYYY') 
        END
    ) AS ngay_sinh_chuan ,

	-- clean ngày vào chuẩn
    CASE 
            WHEN "ngay_vao_lam_(m/d/y)" ~ '^\d{4}-\d{2}-\d{2}' 
            THEN LEFT("ngay_vao_lam_(m/d/y)", 10)::DATE  end as ngay_vao_lam_chuan,

 	-- clean ngày nghỉ chuẩn
	ngay_nghi::DATE  as ngay_nghi_chuan      
     
   from stg.hrm_employees
) select 
	replace(upper(ma_nhan_vien), ' ','') as ma_nhan_vien , 
	initcap(ho_va_ten) as ho_va_ten, 
	initcap(tinh_trang) as tinh_trang, 
	replace(phong_ban, '  ', ' ') as phong_ban, 
	replace(chi_nhanh, '  ', ' ') as chi_nhanh , 
	initcap(chuyen_mon_phu_trach) chuyen_mon_phu_trach, 
	gioi_tinh, 
	ngay_sinh_chuan as ngay_sinh, 
	ngay_vao_lam_chuan as ngay_vao_lam,  
	ngay_nghi_chuan as ngay_nghi
from clean_hrm_employees where ma_nhan_vien is not null );



-- Tạo bảng int.employees
create table int.employees as( 
with source_platforms as (
	select 
		upper(replace(ma_nhan_vien, ' ', ''))  as "ma_nhan_vien",
		'MISA' as nen_tang
	from 
		stg.misa_employees
	union 
	select 
		upper(replace(ma_nhan_vien, ' ', '')) as "ma_nhan_vien",
		'FILE' as nen_tang
	from 
		stg.clean_hrm_employees
	union 
	select
		upper(replace(ma_nhan_vien, ' ', '')) as "ma_nhan_vien",
		'DMS' as nen_tang
	from stg.dms_employees
), 	
	-- Xuất mã nhân viên UNIQUE và tính toán agg string các nền tảng
	all_ma_nhan_vien as (
    -- Gom các nền tảng lại thành một chuỗi: "MISA, FILE, DMS"
    select 
        ma_nhan_vien, 
        string_agg(nen_tang, ', ') as danh_sach_nen_tang 
    from source_platforms
    group by 1) ,
    
    -- Chọn các cột để clean ở file nhân sự
	-- FILE NHÂN SỰ, TẢI TRÊN GOOGLE SHEET
 file_clean as ( 
 select 
 	replace(upper(ma_nhan_vien), ' ','') as ma_nhan_vien,
 	max(ho_va_ten) as ten_nhan_vien,
 	max(tinh_trang) as tinh_trang,
 	max(phong_ban) as phong_ban,
 	max(chi_nhanh) as chi_nhanh,
 	max(chuyen_mon_phu_trach) as chuyen_mon_phu_trach,
 	max(gioi_tinh) as gioi_tinh,
 	max(ngay_vao_lam) as ngay_vao_lam,
 	max(ngay_nghi) as ngay_nghi
 from 
 	stg.clean_hrm_employees
 group by 1 ),
 	-- CLEAN_MISA
 misa_clean as (
 select
 	replace(upper(ma_nhan_vien), ' ','') as ma_nhan_vien,
 	max(ten_nhan_vien) as ten_nhan_vien,
 	max(gioi_tinh) as gioi_tinh,
 	max(ngay_sinh) as ngay_sinh,
 	max(chuc_danh) as chuc_danh,
 	max(ma_don_vi) as ma_don_vi,
 	max(ten_don_vi) as ten_don_vi,
 	max(phong_ban) as phong_ban
 from 
 	stg.misa_employees
 group by 1	
 ),
	-- CLEAN DMS
 dms_clean as (
 select 
 	replace(upper(ma_nhan_vien), ' ','') as ma_nhan_vien,
 	max(ten_nhan_vien) as ten_nhan_vien,
 	max(tai_khoan_dang_nhap) as tai_khoan_dang_nhap,
 	max(so_dien_thoai) as so_dien_thoai,
 	max(ngay_thang_nam_sinh) as ngay_thang_nam_sinh,
 	max(gioi_tinh) as gioi_tinh,
 	max(phong_ban) as phong_ban,
 	max(chuc_danh) as chuc_danh
 from 
 	stg.dms_employees
 group by 1
 ) 
 select 
 	-- lấy mã nhân viên duy nhất
 	a.ma_nhan_vien ,

 	-- set up lại tên nhân viên cho các nv Hà Nội, dùng cho dashboard 
 	replace(regexp_replace(initcap(COALESCE(dms.ten_nhan_vien,m.ten_nhan_vien, duc.ten_nhan_vien )), ' TN$| Bb$|\.$|-T$| \.$','', 'g') , ' Tn', '') as "ten_nhan_vien",

 	-- dùng cho search file từ misa
 	m.ten_nhan_vien as ten_nhan_vien_misa,

 	-- tình trạng làm việc
 	duc.tinh_trang as tinh_trang_lam_viec ,

	-- lấy ngày vào làm trong file của HRM
 	duc.ngay_vao_lam, 

	-- lấy ngày nghỉ trong file HRM
 	duc.ngay_nghi,
 	
 	-- giới tính
 	COALESCE(dms.gioi_tinh, duc.gioi_tinh, m.gioi_tinh ) as "gioi_tinh",
 	
 	-- chỉnh lại tên phòng ban cho các file từ DMS, FILE HRM, MISA cho đồng nhất
 	replace(replace(replace(replace(replace(replace(COALESCE(dms.phong_ban, m.phong_ban, duc.phong_ban), 'Phòng Beauty HN', 'Beauty HN') , 'Phòng Bán Buôn HN', 'Bán Buôn HN'), 'Bán buôn HN', 'Bán Buôn HN'), 'KD3','Phòng KD3'), 'Vận hành', 'Phòng vận hành'), 'VP Đắk Lắk', 'VP Đăk Lăk')  as "phong_ban",
 	
 	-- chuyên môn phụ trách
 	duc.chuyen_mon_phu_trach,
 	
 	-- chức danh
 	UPPER(LEFT(COALESCE( dms.chuc_danh, m.chuc_danh), 1)) || LOWER(SUBSTRING(COALESCE( dms.chuc_danh, m.chuc_danh) from 2)) as "chuc_danh",
	
 	-- tài khoản đăng nhập
	replace(dms.tai_khoan_dang_nhap::varchar(50) , '84', '0') as "tai_khoan_dang_nhap" ,
	
	-- ngày tháng năm sinh
	dms.ngay_thang_nam_sinh,
	
 	-- danh sách nền tảng
 	a.danh_sach_nen_tang
 from 
  	
 	all_ma_nhan_vien as a 
 left join 
 	misa_clean as m on m.ma_nhan_vien = a.ma_nhan_vien
 left join 
 	dms_clean as dms on dms.ma_nhan_vien = a.ma_nhan_vien
 left join 
 	file_clean as duc on duc.ma_nhan_vien = a.ma_nhan_vien
  
 	order by phong_ban asc , tinh_trang_lam_viec asc );
