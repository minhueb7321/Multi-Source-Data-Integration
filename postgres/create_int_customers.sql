drop table if exists int.customers;


drop table if exists stg.clean_dms_customers;
create table stg.clean_dms_customers as (
with base as (
select 
	replace(upper(ma_khach_hang), ' ','') as ma_khach_hang ,
	ten_khach_hang,
	nguoi_phu_trach, 
	phong_ban,
	dia_chi,
	"tinh/thanh" as tinh_thanh,
	so_dien_thoai ,
	replace(loai_kh, 'Salon', 'Beauty Salon') as loai_khach_hang,
	trang_thai_thong_tin,
	dang_khach_hang as dang_khach_hang_DMS ,
	to_timestamp(ngay_chuyen_doi ,'HH24:MI:SS DD/MM/YYYY')::TIMESTAMP as ngay_tao_chuyen_doi,
	'Ngày chuyển đổi' as dang_ngay_tao_chuyen_doi	
	
from 
	stg.dms_dskh
union 
select 
	replace(upper(ma_khach_hang), ' ','') as ma_khach_hang,
	ten_khach_hang,
	'' as nguoi_phu_trach, 
	phong_ban,
	dia_chi,
	"tinh/thanh" as tinh_thanh,
	so_dien_thoai,
	replace(loai_kh, 'Salon', 'Beauty Salon') as loai_khach_hang,
	trang_thai_thong_tin,
	dang_khach_hang as dang_khach_hang_DMS,
	to_timestamp(ngay_chuyen_doi,'HH24:MI:SS DD/MM/YYYY')::TIMESTAMP as ngay_tao_chuyen_doi,
	'Ngày chuyển đổi' as dang_ngay_tao_chuyen_doi
from 
	stg.dms_dskh_chung

union 
select
	replace(upper(ma_tiem_nang), ' ','') as ma_khach_hang,
	ten_tiem_nang as ten_khach_hang,
	nguoi_phu_trach, 
	phong_ban,
	dia_chi,
	"tinh/thanh" as tinh_thanh,
	so_dien_thoai,
	replace(loai_kh, 'Salon', 'Beauty Salon') as loai_khach_hang,
	trang_thai as trang_thai_thong_tin,
	dang_khach_hang as dang_khach_hang_DMS,
	to_timestamp(ngay_tao ,'HH24:MI:SS DD/MM/YYYY')::TIMESTAMP as ngay_tao_chuyen_doi,
	'Ngày tạo' as dang_ngay_tao_chuyen_doi
from 
	stg.dms_dstn
union 
	select 
	replace(upper(ma_tiem_nang), ' ','') as ma_khach_hang,
	ten_tiem_nang as ten_khach_hang,
	'' as nguoi_phu_trach, 
	phong_ban,
	dia_chi,
	"tinh/thanh" as tinh_thanh,
	so_dien_thoai,
	replace(loai_kh, 'Salon', 'Beauty Salon') as loai_khach_hang,
	trang_thai as trang_thai_thong_tin,
	dang_khach_hang as dang_khach_hang_DMS,
	to_timestamp(ngay_tao,'HH24:MI:SS DD/MM/YYYY')::TIMESTAMP as ngay_tao_chuyen_doi,
	'Ngày tạo' as dang_ngay_tao_chuyen_doi	
from 
	stg.dms_dstn_chung
) 
	select *

	from base order by dang_khach_hang_DMS asc );
	
	
drop table if exists int.customers;
create table int.customers as (
with source_platform as (
	select 
		replace(upper(ma_khach_hang), ' ', '') as ma_khach_hang,
		'FILE' as nen_tang
	from stg.duc_dskh
	union 
	select 
		replace(upper(ma_khach_hang), ' ', '') as ma_khach_hang,
		'DMS' as nen_tang
	from
		stg.clean_dms_customers 
	union 
	select
		replace(upper(ma_khach_hang), ' ', '') as ma_khach_hang,
		'MISA' as nen_tang
	from stg.misa_customers
), all_ma_khach_hang as (
		select 
			ma_khach_hang ,
			string_agg(nen_tang, ', ') as danh_sach_nen_tang
		from 
			source_platform
		group by 1
), dms_clean as (
	select 
		ma_khach_hang,
		max(ten_khach_hang) as ten_khach_hang,
		max(nguoi_phu_trach) as nguoi_phu_trach,
		max(phong_ban) as phong_ban,
		max(dia_chi) as dia_chi,
		max(tinh_thanh) as tinh_thanh,
		max(so_dien_thoai) as so_dien_thoai,
		max(loai_khach_hang) as loai_khach_hang,
		max(trang_thai_thong_tin) as trang_thai_thong_tin,
		max(dang_khach_hang_dms) as dang_khach_hang_dms,
		max(ngay_tao_chuyen_doi) as ngay_tao_chuyen_doi,
		max(dang_ngay_tao_chuyen_doi) as dang_ngay_tao_chuyen_doi
	from 
		stg.clean_dms_customers
	group by 1
),
	file_clean as (
	 select 
	 	ma_khach_hang ,
	 	max(ten_khach_hang) as ten_khach_hang,
	 	max(nguoi_phu_trach) as nguoi_phu_trach,
	 	max(phong_ban) as phong_ban,
	 	max(dia_chi) as dia_chi,
	 	max(replace(replace(loai_kh, 'BS', 'Beauty Salon'), 'ĐL', 'Đại lý')) as loai_khach_hang,
	 	max(ngay_chuyen_doi) as ngay_chuyen_doi
		
	 from 
	 	stg.duc_dskh 
	 group by 1
),
	misa_clean as (
	  select 
	  	 ma_khach_hang,
	  	 max(ten_khach_hang) as ten_khach_hang,
	  	 max(dia_chi) as dia_chi,
	  	 max(nhan_vien) as ma_nhan_vien_phu_trach,
	  	 max(ten_nhan_vien) as nguoi_phu_trach,
	  	 max(ngay_tao) as ngay_tao,
	  	 max(phong_ban) as phong_ban
	  from 
	  	stg.misa_customers
	  group by 1
) , clean_stage as (
	select 
		-- FULL MÃ KHÁCH HÀNG
		a.ma_khach_hang,

		-- TÊN KHÁCH HÀNG , thứ tự ưu tiên : dms.ten_khach_hang -> f.ten_khach_hang -> misa.ten_khach_hang,
		COALESCE(dms.ten_khach_hang,f.ten_khach_hang, misa.ten_khach_hang) as ten_khach_hang, misa.ten_khach_hang as ten_khach_hang_misa, 

		-- NGƯỜI PHỤ TRÁCH (Tên nhân viên sale) , thứ tự ưu tiên : dms.nguoi_phu_trach -> misa.nguoi_phu_trach -> f.nguoi_phu_trach,
		coalesce(nullif(dms.nguoi_phu_trach,''), misa.nguoi_phu_trach, f.nguoi_phu_trach) as nguoi_phu_trach,	
	
		-- ĐỊA CHỈ 
		COALESCE(dms.dia_chi, f.dia_chi, misa.dia_chi) as dia_chi,
		
		-- PHÒNG BAN
		COALESCE(dms.phong_ban,misa.phong_ban, f.phong_ban) as phong_ban,
		
		-- TỈNH THÀNH
		COALESCE(dms.tinh_thanh, misa.phong_ban) as tinh_thanh,
		
		-- LOẠI KHÁCH HÀNG
		COALESCE(f.loai_khach_hang, dms.loai_khach_hang) as loai_khach_hang,
		
		-- SỐ ĐIỆN THOẠI + TRẠNG THÁI THÔNG TIN + DẠNG KHÁCH HÀNG (DMS,MISA, FILE)
		dms.so_dien_thoai, dms.trang_thai_thong_tin, dms.dang_khach_hang_dms,
		
		-- NGÀY CHUYỂN ĐỔI + NGÀY TẠO , để thành ngày tạo chuyển đổi
		dms.ngay_tao_chuyen_doi as ngay_tao_chuyen_doi,

		-- NGÀY TẠO MISA
		misa.ngay_tao as ngay_tao_misa,

		-- DẠNG NGÀY TẠO HAY NGÀY CHUYỂN ĐỔI (Ngày tạo, ngày chuyển đổi)
		dms.dang_ngay_tao_chuyen_doi,

		misa.ma_nhan_vien_phu_trach,
		
		-- DANH SÁCH NỀN TẢNG
		a.danh_sach_nen_tang

	from 
		all_ma_khach_hang a
	left join 
		dms_clean as dms on dms.ma_khach_hang = a.ma_khach_hang
	left join 
		file_clean as f on f.ma_khach_hang = a.ma_khach_hang
	left join
		misa_clean as misa on misa.ma_khach_hang = a.ma_khach_hang 
	) 
		select 
			distinct on (c.ma_khach_hang)
			-- nhớ select mã khách hàng
			c.* , 
			coalesce(upper(c.ma_nhan_vien_phu_trach), upper(emp.ma_nhan_vien))as ma_nhan_vien  -- nối với bảng nhân viên để lấy mã nhân viên phụ trách
		from clean_stage as c 
			left join 
		int.employees emp on c.nguoi_phu_trach = emp.ten_nhan_vien or c.nguoi_phu_trach = emp.ten_nhan_vien_misa 
)
	 ;
		
		

	
