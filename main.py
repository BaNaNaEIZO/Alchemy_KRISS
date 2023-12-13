from sqlalchemy import create_engine, ForeignKey, select
from sqlalchemy.orm import mapped_column, declarative_base, Mapped, Session
import pandas as pd

engine = create_engine("sqlite+pysqlite:///:memory:", echo=True)

data_1 = pd.read_csv('cosmetics.csv')

data_1 = data_1.drop(['Combination', 'Dry', 'Normal', 'Oily', 'Sensitive'], axis=1)

unique_values = data_1['Brand'].drop_duplicates(ignore_index=True)
id_i = pd.RangeIndex(len(unique_values))
value_to_id = dict(zip(unique_values, id_i))
brand_new = pd.DataFrame({'Brand': unique_values, 'Brand_ID': id_i})

data_1['Brand_ID'] = data_1['Brand'].map(value_to_id)
data_1 = data_1.drop('Brand', axis=1)

unique_values = data_1['Label'].drop_duplicates(ignore_index=True)
id_i = pd.RangeIndex(len(unique_values))
value_to_id = dict(zip(unique_values, id_i))
label_new = pd.DataFrame({'Label': unique_values, 'Label_ID': id_i})

data_1['Label_ID'] = data_1['Label'].map(value_to_id)
data_1 = data_1.drop('Label', axis=1)

Base = declarative_base()

print(label_new.dtypes)


class CosmeticsTable(Base):
    __tablename__ = "cosmetics"  # noqa
    id: Mapped[int] = mapped_column(primary_key=True)
    Name: Mapped[str] = mapped_column()
    Price: Mapped[int] = mapped_column()
    Rank: Mapped[float] = mapped_column()
    Ingredients: Mapped[str] = mapped_column()
    Brand_ID: Mapped[int] = mapped_column()
    Label_ID: Mapped[int] = mapped_column()


class AddressBrandTable(Base):
    __tablename__ = "adresses_brand"  # noqa
    id: Mapped[int] = mapped_column(primary_key=True)
    Brand: Mapped[str] = mapped_column()
    Brand_ID: Mapped[int] = mapped_column(ForeignKey("cosmetics.Brand_ID"))


class AddressLabelTable(Base):
    __tablename__ = "adresses_lable"  # noqa
    id: Mapped[int] = mapped_column(primary_key=True)
    Label: Mapped[str] = mapped_column()
    Label_ID: Mapped[int] = mapped_column(ForeignKey("cosmetics.Label_ID"))


# # furniture = FurnitureTable(item_id=1, name="waf", price=10, old_price="wad",
# #                                        sellable_online=True, other_colors="wafg",designer="dwa", depth=42, height=41, width=10,
# #                                        category_ID=1)
# # furniture = FurnitureTable(
# #             item_id=90420332, name='FREKVENS', price=265.0, old_price='No old price',
# #             sellable_online=True, other_colors='No', designer='Nicholai Wiig Hansen',
# #             depth=54.379202151501566, height=99.0, width=51.0, category_ID=0)

i = 0
Base.metadata.create_all(engine)

with Session(engine) as f_session:
    with f_session.begin():
        for row in data_1.to_dict(orient='records'):
            i += 1
            furniture = CosmeticsTable(id=i, Name=row["Name"], Price=row["Price"], Rank=row["Rank"],
                                       Ingredients=row["Ingredients"],
                                       Brand_ID=row["Brand_ID"], Label_ID=row["Label_ID"],
                                       )
            f_session.add(furniture)

    with f_session.begin():
        res = f_session.execute(select(CosmeticsTable).where(CosmeticsTable.id == 8))
        cosmetics = res.scalar()
        print(cosmetics.id)

i = 0
with Session(engine) as a_session:
    with a_session.begin():
        for row in brand_new.to_dict(orient='records'):
            i += 1
            address = AddressBrandTable(id=i, Brand=row["Brand"], Brand_ID=row["Brand_ID"])
            a_session.add(address)
    with a_session.begin():
        res = a_session.execute(select(AddressBrandTable).where(AddressBrandTable.id == 3))
        addresses_brand = res.scalar()
        print(addresses_brand.Brand)

i = 0
with Session(engine) as a_session:
    with a_session.begin():
        for row in label_new.to_dict(orient='records'):
            i += 1
            address = AddressLabelTable(id=i, Label=row["Label"], Label_ID=row["Label_ID"])
            a_session.add(address)
    with a_session.begin():
        res = a_session.execute(select(AddressLabelTable).where(AddressLabelTable.id == 4))
        addresses_label = res.scalar()
        print(addresses_label.Label)
