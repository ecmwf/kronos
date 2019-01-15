from kronos_modeller.job_generation.schedule.equiv_time_pdf import TimeSchedulePDF
from kronos_modeller.job_generation.schedule.equiv_time_pdf_exact import TimeSchedulePDFexact

job_schedule_factory = {
    "equiv_time_pdf": TimeSchedulePDF,
    "equiv_time_pdf_exact": TimeSchedulePDFexact,
}