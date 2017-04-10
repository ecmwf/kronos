from kronos.core.time_schedule.equiv_time_pdf import TimeSchedulePDF
from kronos.core.time_schedule.equiv_time_pdf_exact import TimeSchedulePDFexact

factory = {
    "equiv_time_pdf": TimeSchedulePDF,
    "equiv_time_pdf_exact": TimeSchedulePDFexact,
}