import random
from datetime import datetime
from typing import Final

from airflow.sdk import dag, task
from airflow.sdk.definitions.asset import Dataset
from faker import Faker
from sqlalchemy import text

from src.models import Lecturer, Student, Group, Subject, Grade, Base, get_session

university_raw_data = Dataset("university_app://raw_data")
# константы для генерации данных
N_GROUPS: Final[int] = 200
N_STUDENTS_IN_GROUP: Final[int] = 25
N_SUBJECTS: Final[int] = 15


# институты
class Institute:
    def __init__(self, institute: str, departments: tuple[str, ...]):
        self.institute = institute
        self.departments = departments


INSTITUTES: Final[tuple[Institute, ...]] = (
    Institute(
        institute="Институт металлургии, машиностроения и материалообработки",
        departments=(
            "Кафедра литейных процессов и материаловедения",
            "Кафедра металлургии и химических технологий",
            "Кафедра обработки материалов давлением им. М.И. Бояршинова",
            "Кафедра машин и технологий обработки давлением и машиностроения",
            "Кафедра механики",
            "Кафедра проектирования и эксплуатации металлургических машин и оборудования",
        )
    ),
    Institute(
        institute="Институт горного дела и транспорта",
        departments=(
            "Кафедра геологии, маркшейдерского дела и обогащения полезных ископаемых",
            "Кафедра горных машин и транспортно-технологических комплексов",
            "Кафедра логистики и управления транспортными системами",
            "Кафедра разработки месторождений полезных ископаемых",
        )
    ),
    Institute(
        institute="Институт энергетики и автоматизированных систем",
        departments=(
            "Кафедра автоматизированного электропривода и мехатроники",
            "Кафедра теплотехнических и энергетических систем",
            "Кафедра электроники и микроэлектроники",
            "Кафедра электроснабжения промышленных предприятий",
            "Кафедра автоматизированных систем управления",
            "Кафедра бизнес-информатики и информационных технологий",
            "Кафедра вычислительной техники и программирования",
            "Кафедра информатики и информационной безопасности",
        )
    ),
    Institute(
        institute="Институт строительства, архитектуры и искусства",
        departments=(
            "Кафедра промышленного и гражданского строительства",
            "Кафедра урбанистики и инженерных систем",
            "Кафедра архитектуры и изобразительного искусства",
            "Кафедра дизайна",
            "Кафедра художественной обработки материалов",
        )
    ),
    Institute(
        institute="Институт экономики и управления",
        departments=(
            "Кафедра менеджмента и государственного управления",
            "Кафедра права и культурологии",
            "Кафедра философии",
            "Кафедра экономики",
        )
    ),
    Institute(
        institute="Институт гуманитарного образования",
        departments=(
            "Кафедра всеобщей истории",
            "Кафедра иностранных языков по техническим направлениям",
            "Кафедра лингвистики и перевода",
            "Кафедра русского языка как иностранного",
            "Кафедра русского языка, общего языкознания и массовой коммуникации",
            "Кафедра языкознания и литературоведения",
            "Кафедра дошкольного и специального образования",
            "Кафедра педагогического образования и документоведения",
            "Кафедра психологии",
            "Кафедра социальной работы и психолого-педагогического образования",
        )
    ),
    Institute(
        institute="Институт естествознания и стандартизации",
        departments=(
            "Кафедра промышленной экологии и безопасности жизнедеятельности",
            "Кафедра технологии, сертификации и сервиса автомобилей",
            "Кафедра химии",
            "Кафедра прикладной математики и информатики",
            "Кафедра физики",
        )
    ),
    Institute(
        institute="Факультет физической культуры и спортивного мастерства",
        departments=(
            "Кафедра спортивного совершенствования",
            "Кафедра физической культуры",
        )
    ),
)

SUBJECT_POOL = {
    "Общие": [
        "История России", "Философия", "Иностранный язык",
        "Безопасность жизнедеятельности", "Физическая культура и спорт",
        "Русский язык и культура речи", "Правоведение"
    ],
    "Металлургия_Машиностроение": [
        "Теория металлургических процессов", "Металловедение",
        "Сопротивление материалов", "Начертательная геометрия",
        "Оборудование литейных цехов", "Проектирование цехов",
        "Механика жидкости и газа", "Термическая обработка металлов"
    ],
    "Горное_дело_Транспорт": [
        "Общая геология", "Маркшейдерия", "Проектирование карьеров",
        "Транспортная логистика", "Горные машины", "Геомеханика",
        "Обогащение полезных ископаемых", "Взрывное дело"
    ],
    "IT_Автоматизация": [
        "Объектно-ориентированное программирование", "Базы данных",
        "Операционные системы", "Сетевые технологии", "Архитектура ЭВМ",
        "Теория автоматического управления", "Информационная безопасность",
        "Алгоритмы и структуры данных"
    ],
    "Энергетика": [
        "Теоретические основы электротехники (ТОЭ)", "Электрические машины",
        "Электроснабжение предприятий", "Промышленная электроника",
        "Релейная защита", "Теплотехнические системы", "Электропривод"
    ],
    "Строительство_Архитектура": [
        "Строительная механика", "Архитектурное проектирование",
        "Железобетонные конструкции", "Урбанистика", "История архитектуры",
        "Дизайн-проектирование", "Инженерная геодезия", "Строительные материалы"
    ],
    "Экономика_Управление": [
        "Микроэкономика", "Макроэкономика", "Бухгалтерский учет и анализ",
        "Менеджмент", "Государственное управление", "Финансовый аудит",
        "Маркетинг", "Мировая экономика"
    ],
    "Гуманитарные_науки": [
        "Психология личности", "Методика преподавания", "Общее языкознание",
        "Теория перевода", "Социология", "Педагогика", "Литературоведение",
        "Документоведение"
    ],
    "Естествознание": [
        "Прикладная математика", "Общая физика", "Органическая химия",
        "Промышленная экология", "Метрология и стандартизация",
        "Математический анализ", "Экологический мониторинг"
    ],
    "Спорт": [
        "Теория и методика физкультуры", "Спортивная медицина",
        "Биомеханика", "Физиология человека", "Психология спорта",
        "Менеджмент в спорте"
    ]
}

# логика генерации
fake = Faker('ru_RU')


def get_realistic_subject(department_name: str, course: int) -> str:
    """
    Выбирает предмет в зависимости от кафедры и курса студента.
    1-2 курс: 70% шанс на общий предмет.
    3-4 курс: 90% шанс на профильный предмет.
    """
    dep = department_name.lower()

    # Определяем профиль
    if any(word in dep for word in ["металлург", "литей", "машин", "давлен", "механик"]):
        pool_key = "Металлургия_Машиностроение"
    elif any(word in dep for word in ["горн", "геолог", "транспорт", "логистик", "месторожд"]):
        pool_key = "Горное_дело_Транспорт"
    elif any(word in dep for word in ["автоматизир", "информат", "вычислит", "программ", "бизнес-информ"]):
        pool_key = "IT_Автоматизация"
    elif any(word in dep for word in ["энерг", "электро", "тепло"]):
        pool_key = "Энергетика"
    elif any(word in dep for word in ["строит", "архитект", "дизайн", "урбан"]):
        pool_key = "Строительство_Архитектура"
    elif any(word in dep for word in ["эконом", "менедж", "право", "управл"]):
        pool_key = "Экономика_Управление"
    elif any(word in dep for word in ["истори", "лингвист", "психолог", "педагог", "филолог", "язык"]):
        pool_key = "Гуманитарные_науки"
    elif any(word in dep for word in ["физик", "хими", "математ", "эколог", "сервис"]):
        pool_key = "Естествознание"
    elif any(word in dep for word in ["спорт", "физическ"]):
        pool_key = "Спорт"
    else:
        pool_key = "Общие"

    if course <= 2:
        # на младших курсах чаще общие предметы
        is_general = random.random() < 0.7
    else:
        # на старших курсах чаще профильные предметы
        is_general = random.random() < 0.1

    final_pool = SUBJECT_POOL["Общие"] if is_general else SUBJECT_POOL[pool_key]
    return random.choice(final_pool)


def generate_lecturers(n_per_dep: int = 3) -> list[Lecturer]:
    lecturers = []
    for inst in INSTITUTES:
        for dep in inst.departments:
            for _ in range(n_per_dep):
                gender = random.choice(['male', 'female'])
                fname = fake.first_name_male() if gender == 'male' else fake.first_name_female()
                sname = fake.middle_name_male() if gender == 'male' else fake.middle_name_female()
                lname = fake.last_name_male() if gender == 'male' else fake.last_name_female()
                lecturers.append(Lecturer(first_name=fname, second_name=sname, last_name=lname, department=dep))

    return lecturers


def generate_groups(n_groups: int = N_GROUPS) -> list[Group]:
    return [
        Group(
            number=100 + i, course=random.randint(1, 4),
            institute=(inst := random.choice(INSTITUTES)).institute,
            department=random.choice(inst.departments)
        ) for i in range(1, n_groups + 1)
    ]


def generate_subjects(groups: list[Group], lecturers: list[Lecturer]) -> list[Subject]:
    subjects = []
    dep_course_pairs = sorted(list({(g.department, g.course) for g in groups}))

    for dep, course in dep_course_pairs:
        dep_lecturers = [l for l in lecturers if l.department == dep]

        if not dep_lecturers:
            continue

        for i in range(N_SUBJECTS):
            semester = (course - 1) * 2 + (i % 2) + 1
            subjects.append(Subject(
                name=f"{get_realistic_subject(dep, course)} (Сем. {semester})",
                semester=semester, hours=random.choice([36 * h for h in range(1, 8)]),
                assessment_type=random.choice(["Экзамен", "Зачет"]),
                lecturer_id=random.choice(dep_lecturers).id
            ))
    return subjects


def generate_students(groups: list[Group]) -> list[Student]:
    students = []
    for group in groups:

        for _ in range(N_STUDENTS_IN_GROUP):
            gender = random.choice(['male', 'female'])
            fname = fake.first_name_male() if gender == 'male' else fake.first_name_female()
            sname = fake.middle_name_male() if gender == 'male' else fake.middle_name_female()
            lname = fake.last_name_male() if gender == 'male' else fake.last_name_female()

            students.append(Student(
                first_name=fname, second_name=sname, last_name=lname,
                birth_date=fake.date_of_birth(minimum_age=17, maximum_age=25),
                group_id=group.id, admission_year=2026 - group.course,
                education_form=random.choice(["Очная", "Заочная"])
            ))

    return students


def generate_grades(students: list[Student], subjects: list[Subject], lecturers: list[Lecturer]) -> list[Grade]:
    grades = []
    lecturer_dep_map = {l.id: l.department for l in lecturers}
    dep_course_subjects_map = {}

    for sub in subjects:
        dep = lecturer_dep_map.get(sub.lecturer_id)
        # Вычисляем курс студента
        course = (sub.semester + 1) // 2
        dep_course_subjects_map.setdefault((dep, course), []).append(sub)

    # Чем старше курс, тем выше шанс получить 4 и 5
    exam_weights_by_course = {
        1: [0.08, 0.32, 0.45, 0.15],  # 1 курс
        2: [0.05, 0.25, 0.45, 0.25],  # 2 курс
        3: [0.02, 0.15, 0.50, 0.33],  # 3 курс
        4: [0.01, 0.05, 0.40, 0.54]  # 4 курс
    }

    credit_weights_by_course = {
        1: [0.85, 0.15],  # 1 курс: 15% незачетов
        2: [0.92, 0.08],  # 2 курс: 8% незачетов
        3: [0.97, 0.03],  # 3 курс: 3% незачетов
        4: [0.99, 0.01]  # 4 курс: 1% незачетов
    }

    for student in students:
        key = (student.group.department, student.group.course)
        for sub in dep_course_subjects_map.get(key, []):

            # Определяем курс для предмета
            sub_course = min((sub.semester + 1) // 2, 4)

            if sub.assessment_type == "Экзамен":
                weights = exam_weights_by_course[sub_course]
                score = str(random.choices([2, 3, 4, 5], weights=weights)[0])
            else:
                weights = credit_weights_by_course[sub_course]
                score = random.choices(["Зачет", "Незачет"], weights=weights)[0]

            grades.append(Grade(
                student_id=student.id,
                subject_id=sub.id,
                date=fake.date_between(start_date='-1y', end_date='today'),
                grade=score
            ))

    return grades


# dag
@dag(
    dag_id="university_data_generator",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["generator", "university"]
)
def university_data_generator():
    @task(outlets=[university_raw_data])
    def generate_and_load_data():
        # Открываем сессию
        with get_session() as session:

            session.execute(text("CREATE SCHEMA IF NOT EXISTS mart;"))
            session.commit()
            try:
                truncate_query = text("""
                                      ;
                                      DELETE FROM public.grades;
                                      DELETE FROM public.students;
                                      DELETE FROM public.subjects;
                                      DELETE FROM public.lecturers;
                                      DELETE FROM public.groups;
    
                                      ALTER SEQUENCE public.students_id_seq RESTART WITH 1;
                                      ;
                                      """)

                session.execute(truncate_query)

                session.commit()
            except Exception:
                pass

            engine = session.get_bind()
            Base.metadata.create_all(engine)

            Faker.seed(random.randint(1, 10000))

            # Лекторы и Группы
            lecturers = generate_lecturers()
            groups = generate_groups()
            session.add_all(lecturers)
            session.add_all(groups)

            # чтобы подтянулись айдишники
            session.flush()

            # Предметы и Студенты
            subjects = generate_subjects(groups, lecturers)
            students = generate_students(groups)
            session.add_all(subjects)
            session.add_all(students)

            # чтобы подтянулись айдишники студентов и предметов
            session.flush()

            # Оценки
            grades = generate_grades(students, subjects, lecturers)
            session.add_all(grades)

            # завершаем транзакцию
            session.commit()

            print(f"добавлено: {len(lecturers)} преподавателей, {len(groups)} групп, "
                  f"{len(subjects)} предметов, {len(students)} студентов, {len(grades)} оценок.")

    generate_and_load_data()

university_data_generator()
