from contextlib import contextmanager
from datetime import date
from typing import Optional, List, Generator, Any
from sqlalchemy import Integer, String, Date, ForeignKey, CheckConstraint, create_engine, Float, Boolean
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker
from sqlalchemy.orm.session import Session

from src.config import Settings


# Базовый класс для всех моделей
class Base(DeclarativeBase):
    pass


@contextmanager
def get_session() -> Generator[Session, Any, None]:
    settings = Settings()
    engine = create_engine(settings.database_url(), echo=False)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    session = SessionLocal()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        engine.dispose()


class Group(Base):
    __tablename__ = "groups"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    number: Mapped[int] = mapped_column(Integer, nullable=False)
    course: Mapped[int] = mapped_column(Integer, nullable=False)
    institute: Mapped[str] = mapped_column(String(255), nullable=False)
    department: Mapped[str] = mapped_column(String(255), nullable=False)

    students: Mapped[List["Student"]] = relationship("Student", back_populates="group")


class Student(Base):
    __tablename__ = "students"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    first_name: Mapped[str] = mapped_column(String(100), nullable=False)
    second_name: Mapped[str] = mapped_column(String(100), nullable=False)
    last_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    birth_date: Mapped[date] = mapped_column(Date, nullable=False)
    admission_year: Mapped[int] = mapped_column(Integer, nullable=False)

    # Ограничение, что форма обучения точно есть
    education_form: Mapped[str] = mapped_column(
        String(7),
        CheckConstraint("education_form IN ('Очная', 'Заочная')"),
        nullable=False
    )

    group_id: Mapped[int] = mapped_column(Integer, ForeignKey("groups.id", ondelete="CASCADE"), nullable=False)

    group: Mapped["Group"] = relationship("Group", back_populates="students")
    grades: Mapped[List["Grade"]] = relationship("Grade", back_populates="student")


class Lecturer(Base):
    __tablename__ = "lecturers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    first_name: Mapped[str] = mapped_column(String(100), nullable=False)
    second_name: Mapped[str] = mapped_column(String(100), nullable=False)
    last_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    department: Mapped[str] = mapped_column(String(255), nullable=False)

    # Связь с предметами, которые ведет лектор
    subjects: Mapped[List["Subject"]] = relationship("Subject", back_populates="lecturer")


class Subject(Base):
    __tablename__ = "subjects"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    semester: Mapped[int] = mapped_column(Integer, nullable=False)
    hours: Mapped[int] = mapped_column(Integer, nullable=False)

    # Ограничение для типа контроля
    assessment_type: Mapped[str] = mapped_column(
        String(7),
        CheckConstraint("assessment_type IN ('Экзамен', 'Зачет')"),
        nullable=False
    )

    lecturer_id: Mapped[int] = mapped_column(Integer, ForeignKey("lecturers.id", ondelete="SET NULL"), nullable=True)

    # Связи
    lecturer: Mapped[Optional["Lecturer"]] = relationship("Lecturer", back_populates="subjects")
    grades: Mapped[List["Grade"]] = relationship("Grade", back_populates="subject")


class Grade(Base):
    __tablename__ = "grades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    date: Mapped[date] = mapped_column(Date, nullable=False)

    # используем тут строку т.к есть зачет, незачет
    grade: Mapped[str] = mapped_column(
        String(7),
        CheckConstraint("grade IN ('2', '3', '4', '5', 'Зачет', 'Незачет')"),
        nullable=False
    )

    student_id: Mapped[int] = mapped_column(Integer, ForeignKey("students.id", ondelete="CASCADE"), nullable=False)
    subject_id: Mapped[int] = mapped_column(Integer, ForeignKey("subjects.id", ondelete="CASCADE"), nullable=False)

    # Связи
    student: Mapped["Student"] = relationship("Student", back_populates="grades")
    subject: Mapped["Subject"] = relationship("Subject", back_populates="grades")


class StudentPerformance(Base):
    __tablename__ = "student_performance"
    __table_args__ = {"schema": "mart"}

    student_id: Mapped[int] = mapped_column(Integer, ForeignKey("students.id", ondelete="CASCADE"), nullable=False, primary_key=True)
    group_id: Mapped[int] = mapped_column(Integer, ForeignKey("groups.id", ondelete="CASCADE"), nullable=False, primary_key=True)

    # Средний балл студента по всем предметам, по которым получены оценки. Выбрано учитывать только числовые оценки, без зачетов
    avg_grade: Mapped[float] = mapped_column(Float, nullable=False)
    # Общее количество зачётных единиц (часов), успешно набранных студентом. Успешной считается оценка не ниже удовлетворительной (>=3) или зачёт.
    total_credits: Mapped[int] = mapped_column(Integer, nullable=False)
    # Количество академических задолженностей предметов, по которым у студента отсутствует положительная оценка или есть незачет.
    debt_count: Mapped[int] = mapped_column(Integer, nullable=False)
    # Признак права на получение стипендии. Обычно стипендия назначается при отсутствии задолженностей и среднем балле не ниже определённого порога (например, >=4.0).
    scholarship_eligible: Mapped[bool] = mapped_column(Boolean, nullable=False)

    student: Mapped["Student"] = relationship("Student")
    group: Mapped["Group"] = relationship("Group")

class GroupStats(Base):
    __tablename__ = "group_stats"
    __table_args__ = {"schema": "mart"}

    group_id: Mapped[int] = mapped_column(Integer, ForeignKey("groups.id", ondelete="CASCADE"), nullable=False, primary_key=True)

    # Курс, на котором находится группа.
    course: Mapped[int] = mapped_column(Integer, nullable=False)

    # Институт, к которому относится группа.
    institute: Mapped[str] = mapped_column(String(255), nullable=False)

    # Средний балл по группе
    avg_grade_group: Mapped[float] = mapped_column(Float, nullable=False)

    # Доля студентов в группе, не имеющих академических задолженностей (успевающих). Процент успешно сдавших сессию.
    pass_rate: Mapped[float] = mapped_column(Float, nullable=False)

    # Предмет, по которому у группы самый высокий средний балл.
    best_subject: Mapped[str] = mapped_column(String(255), nullable=False)
    # Предмет, по которому у группы самый низкий средний балл.
    worst_subject: Mapped[str] = mapped_column(String(255), nullable=False)
