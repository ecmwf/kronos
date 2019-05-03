from kronos_modeller.workload_modelling.time_schedule.time_schedule_pdf_exact import TimeSchedulePDFexact
from kronos_modeller.workload_modelling.time_schedule.time_schedule_pdf import TimeSchedulePDF

job_schedule_factory = {
    "equiv_time_pdf": TimeSchedulePDF,
    "equiv_time_pdf_exact": TimeSchedulePDFexact,
}