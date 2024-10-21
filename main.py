import sqlalchemy as sq
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
import json


DSN = "postgresql://postgres:123456@localhost:5432/netology_orm"
engine = sq.create_engine(DSN)

Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()


class Publisher(Base):
    __tablename__ = 'publisher'

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String, unique=True)

    def __str__(self):
        return f'Publisher: {self.id}:{self.name}'


class Book(Base):
    __tablename__ = 'book'

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String, nullable=False)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey('publisher.id'), nullable=False)

    publisher = relationship("Publisher", backref="books")

    def __str__(self):
        return f'Book: {self.id}:{self.title}'


class Shop(Base):
    __tablename__ = 'shop'

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String, unique=True)

    def __str__(self):
        return f'Shop: {self.id}:{self.name}'


class Stock(Base):
    __tablename__ = 'stock'

    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey('book.id'), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey('shop.id'), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)

    book = relationship("Book", backref="stock")
    shop = relationship("Shop", backref="stock")

class Sale(Base):
    __tablename__ = 'sale'

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Float, nullable=False)
    date_sale = sq.Column(sq.Date, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey('stock.id'), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)

    publisher = relationship("Stock", backref="sale")

    def __str__(self):
        return f'Sale: {self.price}:{self.data_sale}'


def create_tables(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

def loading_data():
    with open('tests_data.json') as fd:
        data = json.load(fd)

    for record in data:
        model = {
            'publisher': Publisher,
            'shop': Shop,
            'book': Book,
            'stock': Stock,
            'sale': Sale,
        }[record.get('model')]
        session.add(model(id=record.get('pk'), **record.get('fields')))

rq = (session.query(Book.title, Shop.name, Sale.price, Sale.date_sale,)
      .select_from(Shop)
      .join(Stock)
      .join(Book)
      .join(Publisher)
      .join(Sale))

def search_publisher(request_publisher):
    if request_publisher.isdigit():
        rp = rq.filter(Publisher.id == request_publisher).all()
        '''Обращаемся к запросу, который составили ранее,
        и применяем фильтрацию, где айди публициста равно переданным данным в функцию, и сохраняем в переменную'''
    else:
        rp = rq.filter(Publisher.name == request_publisher).all()
        '''Обращаемся к запросу, который составили ранее,
        и применяем фильтрацию, где имя публициста равно переданным данным в функцию, и сохраняем в переменную'''
    for Book, Shop, Sale, Date_Sale in rp:
        '''Проходим в цикле по переменой, в которой сохраняем результат фильтрации,
        и при каждой итерации получаем кортеж и распаковываем значения в 4 переменные'''
        print(f"{Book: <40} | {Shop: <10} | {Sale: <8} | {Date_Sale.strftime('%d-%m-%Y')}")
        '''Передаем в форматированную строку переменные, которые содержат имя книги, название магазина,
        стоимость продажи и дату продажи'''

create_tables(engine)
loading_data()

# search_publisher('O\u2019Reilly')
# search_publisher('Pearson')
# search_publisher('Microsoft Press')
# search_publisher('No starch press')

session.close()