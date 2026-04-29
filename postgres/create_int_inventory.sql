-------------------------------------------------------------
drop table if exists int.detail_inventory;
create table int.detail_inventory as (
with source_flatform as (
	select 
		distinct(upper("phieu/don_hang")) as "ma_don_hang",
		'FILE_INVENTORY' as "nen_tang" 
	from 
		stg.dms_inventory_history 
union
	select 
		distinct(upper(ma_don_hang)) as "ma_don_hang",
		'FILE_ORDER' as "nen_tang" 
	from 	
		stg.dms_orders
), 
	all_ma_van_don as (
	select 
		ma_don_hang,
		string_agg(nen_tang, ', ') as danh_sach_nen_tang
	from 	
		source_flatform
	group by 1
),
	clean_stage as (
	select
		a.ma_don_hang as "phieu_don_hang",
		
		-- inventory history
		-- thông tin các đối tượng
		his.ten_kho_hang, 
		his.doi_tuong_lien_quan,
		o."khach_hang/tiem_nang",
		concat(his.ten_kho_hang , ' - ', his.doi_tuong_lien_quan) as "nguon_goc_bien_dong",
		o.ma_don_hang as "ma_don_dat_hang",
		o."ma_khach_hang/tiem_nang" as "ma_khach_hang",
		case 
			when o."ma_khach_hang/tiem_nang" like ('%_DL_%') then 'Đại lý'
			when o."ma_khach_hang/tiem_nang" like ('%_BS_%') then 'Beauty Salon'
		end as "loai_khach_hang", 
		
		c.phong_ban, 
		c.tinh_thanh,

		his.thoi_gian_phat_sinh,
		
		
		-- phần sản phẩm
		his.ma_san_pham,
		case 
			when his.doi_tuong_lien_quan like ('%Mượn Hàng Mẫu%') then 'Hàng mẫu' else 'Hàng bán' end as "phan_loai_hang",
			
		his.ten_san_pham,
		his.hinh_thuc,
		his."dung_tich_-_khoi_luong" as "dung_tich_khoi_luong",
		his.so_luong ,
		p.gia_niem_yet, -- giá niêm yết
		
	
		-- coalesce() outbound and inbound
		coalesce(out.loai_phieu_xuat, inb.loai_phieu_nhap) as "loai_phieu_nhap_xuat",
		coalesce(out.nguoi_tao, inb.nguoi_tao) as "nguoi_tao_phieu_nhap_xuat",
		coalesce(out.thoi_gian_tao, inb.thoi_gian_tao) as "thoi_gian_tao_nhap_xuat",
		coalesce(out.trang_thai, inb.trang_thai) as "trang_thai_nhap_xuat",
		
		-- orders
		
		o.so_luong_san_pham as "tong_luong_ban",
		o.tong_tien as "tong_tien_ban",
		o.chiet_khau as "tong_chiet_khau",
		o.tien_thanh_toan as "tong_tien_thanh_toan",
		o.nguoi_tao as "nguoi_tao_don_hang",
		o.trang_thai as "trang_thai_don_hang_ban",
		o.ngay_tao as "ngay_tao_don_hang_ban",
			
		
		lad.ma_van_don as "ma_van_don",
		lad.van_chuyen as "nguoi_van_chuyen" ,
--		lad.tien_thanh_toan,
		lad.trang_thai as "trang_thai_don_van_chuyen",
		lad.ngay_tao as "ngay_tao_van_don",
			
		a.danh_sach_nen_tang
	from 	
		all_ma_van_don as a
	left join	
		stg.dms_inventory_history as his on  his."phieu/don_hang" = a.ma_don_hang
	left join
		int.products as p on p.ma_san_pham = his.ma_san_pham 
	left join
		stg.dms_inbound as inb on inb.ma_phieu = a.ma_don_hang
	left join
		stg.dms_outbound as out on out.ma_phieu = a.ma_don_hang
	left join
		stg.dms_orders as o on o.ma_don_hang = a.ma_don_hang
	left join 
		stg.dms_lading as lad on  lad.ma_van_don = o.van_don
	left join 	
		int.customers c on c.ma_khach_hang = o."ma_khach_hang/tiem_nang"
		)
	select 
		* from clean_stage) ;


