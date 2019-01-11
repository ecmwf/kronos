from kronos_modeller.job_generation import TimeSchedulePDF
from kronos_modeller.job_generation import TimeSchedulePDFexact

job_schedule_factory = {
    "equiv_time_pdf": TimeSchedulePDF,
    "equiv_time_pdf_exact": TimeSchedulePDFexact,
}