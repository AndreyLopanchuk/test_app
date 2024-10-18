import random
import sys
import time
from datetime import datetime
from functools import wraps
from typing import List, Union

from faker import Faker

from database import Base, engine, Employee, session

fake = Faker()


def execution_time(func):
    """
    Декоратор для измерения времени выполнения функции.

    Измеряет время, затраченное на выполнение функции, и выводит результат в консоль.

    :param func: Функция, время выполнения которой нужно измерить
    :type func: callable
    :return: Результат выполнения функции
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        executions_time = round(end_time - start_time, 4)
        print(f"Время выполнения функции {func.__name__}: {executions_time} секунд")
        return result
    return wrapper


def create_table() -> None:
    """
    Создаёт таблицы в базе данных.

    Использует метаданные SQLAlchemy для создания таблиц в базе данных.
    Если таблицы уже существуют, они не будут пересозданы.

    :return: None
    """
    Base.metadata.create_all(engine, checkfirst=True)
    print('table created')


def add_employee(argv: list) -> None:
    """
    Добавляет нового сотрудника в базу данных.

    Создаёт новый объект сотрудника на основе переданных аргументов,
    добавляет его в базу данных и выводит возраст сотрудника.

    :param argv: Список аргументов, содержащий полное имя, дату рождения и пол сотрудника
    :type argv: list[str]
    :return: None
    """
    employee = Employee(full_name=argv[0],
                        birth_date=datetime.strptime(argv[1], "%Y-%m-%d").date(),
                        sex=argv[2])
    employee.send_to_db()
    print(f'employee added. age: {employee.calculate_age()}')


def get_unique_employees() -> None:
    """
    Выводит список уникальных сотрудников.

    Выполняет запрос к базе данных, группируя сотрудников по полному имени и дате рождения,
    и выводит результат в консоль.

    :return: None
    """
    result = (session.query(Employee.full_name, Employee.sex, Employee.birth_date)
              .group_by(Employee.full_name, Employee.birth_date).all())
    for row in result:
        print(f'full_name: {row.full_name}, sex: {row.sex}, birth_date: {row.birth_date}')


def generate_employees(quantity: int = 100,
                       sex_default: Union[str, None] = None,
                       first_latter_default: Union[str, None] = None) -> List[Employee]:
    """
    Генерирует список сотрудников.

    Генерирует список сотрудников с случайными данными, включая полное имя, пол, и дату рождения.
    Количество сотрудников определяется параметром quantity.
    Если параметр sex_default не указан, пол сотрудников выбирается случайно.
    Если параметр first_latter_default не указан, фамилия сотрудника начинается с случайной буквы.

    :param quantity: Количество сотрудников
    :type quantity: int
    :param sex_default: По умолчанию используется пол сотрудника
    :type sex_default: Union[str, None]
    :param first_latter_default: По умолчанию фамилия сотрудника начинается с случайной буквы
    :type first_latter_default: Union[str, None]
    :return: Список сотрудников
    :rtype: List[Employee]
    """
    employees = []
    sex = sex_default
    for _ in range(quantity):
        full_name = fake.last_name() + ' ' + fake.first_name() + ' ' + fake.first_name()
        if first_latter_default is not None:
            full_name = first_latter_default + full_name[1:]
        if sex_default is None:
            sex = random.choice(['Male', 'Female'])
        birth_date = fake.date_between(start_date='-70y', end_date='-18y')
        employee = Employee(full_name=full_name, sex=sex, birth_date=birth_date)
        employees.append(employee)
    return employees


@execution_time
def generate_fake_employees() -> None:
    """
    Генерирует и добавляет в базу данных фейковые данные о сотрудниках.

    Генерирует 1 000 000 сотрудников с случайными данными и добавляет их в базу данных.
    Затем генерирует еще 100 сотрудников с полом 'Male' и именем, начинающимся с буквы 'F', и добавляет их в базу данных.

    :return: None
    """
    employees = generate_employees(1000000)
    Employee.bulk_insert(employees)
    print(f'1000000 employees added')

    employees = generate_employees(100, 'Male', 'F')
    Employee.bulk_insert(employees)
    print(f'100 employees added')


@execution_time
def fetch_data() -> None:
    """
    Извлекает данные о сотрудниках из базы данных.

    Выполняет запрос к базе данных, извлекая сотрудников с полом 'Male' и именем, начинающимся с буквы 'F'.
    Выводит количество найденных записей.

    :return: None
    """
    data = session.query(Employee).filter(Employee.sex == 'Male', Employee.full_name.like('F%')).all()
    print(len(data))

command_dict = {
    '1': create_table,
    '2': add_employee,
    '3': get_unique_employees,
    '4': generate_fake_employees,
    '5': fetch_data,
    '6': fetch_data,
}

def main():
    if len(sys.argv) > 1 and sys.argv[1] in command_dict:
        data = sys.argv[2:]
        if data:
            command_dict[sys.argv[1]](data)
        else:
            command_dict[sys.argv[1]]()
    else:
        print("invalid command")

if __name__ == "__main__":
    main()