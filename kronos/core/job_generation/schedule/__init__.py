from kronos.core.job_generation.schedule.equiv_time_pdf_exact import TimeSchedulePDFexact
from kronos.core.job_generation.schedule.equiv_time_pdf import TimeSchedulePDF

job_schedule_factory = {
    "equiv_time_pdf": TimeSchedulePDF,
    "equiv_time_pdf_exact": TimeSchedulePDFexact,
}