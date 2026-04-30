# backend/app/db/models/stock.py
from datetime import date

from sqlalchemy import Date, Float, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class Stock(Base, TimestampMixin):
    """股票基础信息表 - 涵盖QMT所有可用字段"""

    __tablename__ = "stocks"

    # === 基础信息 ===
    symbol: Mapped[str] = mapped_column(String(20), primary_key=True, comment="股票代码")
    name: Mapped[str | None] = mapped_column(String(50), comment="股票名称")
    exchange: Mapped[str | None] = mapped_column(String(10), comment="交易所: SH/SZ/BJ")

    # === 分类信息 ===
    industry: Mapped[str | None] = mapped_column(String(50), comment="所属行业(申万一级)")
    industry2: Mapped[str | None] = mapped_column(String(50), comment="所属行业(申万二级)")
    industry3: Mapped[str | None] = mapped_column(String(50), comment="所属行业(申万三级)")
    sector: Mapped[str | None] = mapped_column(String(50), comment="板块: 主板/创业板/科创板/北交所")
    concept: Mapped[str | None] = mapped_column(Text, comment="概念板块(逗号分隔)")

    # === 上市信息 ===
    list_date: Mapped[date | None] = mapped_column(Date, comment="上市日期")
    delist_date: Mapped[date | None] = mapped_column(Date, comment="退市日期")
    is_st: Mapped[int] = mapped_column(Integer, default=0, comment="是否ST: 0-否, 1-ST, 2-*ST")
    is_delist: Mapped[int] = mapped_column(Integer, default=0, comment="是否退市: 0-否, 1-是")
    is_suspend: Mapped[int] = mapped_column(Integer, default=0, comment="是否停牌: 0-否, 1-是")

    # === 股本结构(单位: 万股) ===
    total_shares: Mapped[float | None] = mapped_column(Float, comment="总股本(万股)")
    float_shares: Mapped[float | None] = mapped_column(Float, comment="流通股本(万股)")
    a_float_shares: Mapped[float | None] = mapped_column(Float, comment="A股流通股本(万股)")
    limit_sell_shares: Mapped[float | None] = mapped_column(Float, comment="限售流通股(万股)")

    # === 市值信息(单位: 万元) ===
    total_mv: Mapped[float | None] = mapped_column(Float, comment="总市值(万元)")
    circ_mv: Mapped[float | None] = mapped_column(Float, comment="流通市值(万元)")

    # === 公司基本信息 ===
    company_name: Mapped[str | None] = mapped_column(String(100), comment="公司全称")
    company_name_en: Mapped[str | None] = mapped_column(String(200), comment="公司英文名称")
    province: Mapped[str | None] = mapped_column(String(20), comment="注册省份")
    city: Mapped[str | None] = mapped_column(String(20), comment="注册城市")
    office_addr: Mapped[str | None] = mapped_column(String(200), comment="办公地址")
    business_scope: Mapped[str | None] = mapped_column(Text, comment="经营范围")
    main_business: Mapped[str | None] = mapped_column(Text, comment="主营业务")
    website: Mapped[str | None] = mapped_column(String(200), comment="公司网址")
    employees: Mapped[int | None] = mapped_column(Integer, comment="员工人数")

    # === 财务指标(最新) ===
    eps: Mapped[float | None] = mapped_column(Float, comment="每股收益(元)")
    bvps: Mapped[float | None] = mapped_column(Float, comment="每股净资产(元)")
    roe: Mapped[float | None] = mapped_column(Float, comment="净资产收益率(%)")
    pe_ttm: Mapped[float | None] = mapped_column(Float, comment="市盈率TTM")
    pb: Mapped[float | None] = mapped_column(Float, comment="市净率")
    total_assets: Mapped[float | None] = mapped_column(Float, comment="总资产(万元)")
    total_liability: Mapped[float | None] = mapped_column(Float, comment="总负债(万元)")
    total_equity: Mapped[float | None] = mapped_column(Float, comment="股东权益(万元)")
    net_profit: Mapped[float | None] = mapped_column(Float, comment="净利润(万元)")
    revenue: Mapped[float | None] = mapped_column(Float, comment="营业收入(万元)")

    # === 其他属性 ===
    security_type: Mapped[str | None] = mapped_column(String(20), comment="证券类型")
    product_class: Mapped[str | None] = mapped_column(String(50), comment="产品类别")
