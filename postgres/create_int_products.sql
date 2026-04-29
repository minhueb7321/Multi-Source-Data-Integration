
drop table if exists int.products;

create table int.products as (
with source_platform as (
	select 
		distinct replace(upper(ma_noi_bo), ' ','') as "ma_san_pham",
		'DMS' as nen_tang
	from
		stg.dms_products
		union 
	select 
		distinct replace(upper(ma), ' ','') as "ma_san_pham",
		'MISA' as nen_tang
	from 
		stg.misa_products
		union 
	select 
		distinct replace(upper(ma_san_pham), ' ','') as "ma_san_pham",
		'FILE' as nen_tang
	from 
		stg.duc_dssp
		
), base as (
	select 
		ma_san_pham, 
		string_agg(nen_tang, ', ') as danh_sach_nen_tang 
	from 	
		source_platform
	group by 1
),
duc_clean AS (
    SELECT 
    	UPPER(ma_san_pham) as ma_san_pham, 
    	MAX(ten_san_pham) as ten_san_pham ,
    	MAX(trang_thai) as "trang_thai",
    	MAX(don_vi_tinh) as "don_vi_tinh",
    	MAX(thuong_hieu) as "thuong_hieu",
    	MAX(nhom_cap_2) as nhom_vat_tu,
    	MAX(ten_nhom_vthh) as ten_nhom_vthh
    FROM stg.duc_dssp GROUP BY 1
),
misa_clean AS (
    SELECT 
    	UPPER(ma) as ma, 
    	MAX(ten) as ten ,
    	MAX(dvt_chinh) as "dvt_tinh",
    	max(ngung_theo_doi) as "trang_thai"
    FROM stg.misa_products GROUP BY 1
),
dms_clean AS (
    SELECT 
    	UPPER(ma_noi_bo) as ma_noi_bo, 
    	MAX(ten_san_pham) as ten_san_pham ,
    	MAX(thuong_hieu) as thuong_hieu,
    	MAX(gia_niem_yet) as gia_niem_yet,
    	MAX("dung_tich_-_khoi_luong") as dung_tich_ml,
    	MAX(danh_muc) as danh_muc
    FROM stg.dms_products GROUP BY 1
)
SELECT 
	-- mã sản phẩm từ base
    b.ma_san_pham,
    
    -- tên sản phẩm
    COALESCE(dms.ten_san_pham, mis.ten, duc.ten_san_pham) AS "ten_san_pham",
    mis.ten as ten_san_pham_misa,
    
    -- trạng thái
    duc.trang_thai, 
    
    
    -- thương hiệu
    coalesce(dms.thuong_hieu, duc.thuong_hieu) as "thuong_hieu",
    
    -- đơn vị tính
    COALESCE(duc.don_vi_tinh, mis.dvt_tinh) AS "don_vi_tinh",
    
    -- gia niêm yết
    replace(replace(dms.gia_niem_yet, 'VND', ''), '.', '')::INT as gia_niem_yet,
    
	-- trích xuất dung tích
    replace(replace(replace(dms.dung_tich_ml, 'ml', ''), 'g', ''), '.', '')::INT as dung_tich_ml,
    
    -- danh mục, nhóm vật tư, tên nhóm vthh lấy ở bên Misa
    duc.nhom_vat_tu as phan_loai_1,
    dms.danh_muc as phan_loai_2,
    duc.ten_nhom_vthh as phan_loai_3,
    
    -- danh sách nền tảng
    b.danh_sach_nen_tang
    
	FROM base AS b
		LEFT JOIN duc_clean duc ON b.ma_san_pham = duc.ma_san_pham
		LEFT JOIN misa_clean mis ON b.ma_san_pham = mis.ma
		LEFT JOIN dms_clean dms ON b.ma_san_pham = dms.ma_noi_bo
	order by duc.trang_thai asc , thuong_hieu asc
);


--DONE



