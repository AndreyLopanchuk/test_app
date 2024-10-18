from datetime import datetime

from sqlalchemy import Column, Integer, String, Date, Enum, create_engine, Index, func
from sqlalchemy.orm import sessionmaker, declarative_base

engine = create_engine('sqlite:///test_app.db')
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

class Employee(Base):
    """
    Класс представляет сотрудника компании.

    Атрибуты:
        id (int): Уникальный идентификатор сотрудника.
        full_name (str): Полное имя сотрудника.
        birth_date (date): Дата рождения сотрудника.
        sex (str): Пол сотрудника ('Мужской' или 'Женский').
    """
    __tablename__ = 'employees'

    id = Column(Integer, primary_key=True)
    full_name = Column(String(50), nullable=False)
    birth_date = Column(Date, nullable=False)
    sex = Column(Enum('Male', 'Female'), nullable=False)

    __table_args__ = (Index('idx_full_name', 'full_name'), )

    def __init__(self, full_name, birth_date, sex):
        self.full_name = full_name
        self.birth_date = birth_date
        self.sex = sex

    def send_to_db(self):
        """
        Отправляет объект в базу данных.

        Добавляет текущий объект в сессию и сохраняет изменения в базе данных.
        """
        session.add(self)
        session.commit()

    def calculate_age(self):
        """
        Вычисляет возраст сотрудника.

        Рассчитывает возраст сотрудника на основе текущей даты и даты рождения.
        Возвращает возраст в годах.

        :return: Возраст сотрудника
        :rtype: int
        """
        today = datetime.today()
        age = today.year - self.birth_date.year
        if (today.month, today.day) < (self.birth_date.month, self.birth_date.day):
            age -= 1
        return age

    @classmethod
    def bulk_insert(cls, employees):
        """
        Выполняет массовую вставку сотрудников в базу данных.

        Использует метод bulk_save_objects для вставки нескольких объектов сотрудников в базу данных за один запрос.
        Сохраняет изменения в базе данных.

        :param employees: Список объектов сотрудников для вставки
        :type employees: list[Employee]
        """
        session.bulk_save_objects(employees)
        session.commit()
