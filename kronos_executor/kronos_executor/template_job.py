
import jinja2
import os

from kronos_executor.external_job import UserAppJob

class TemplateMixin:

    default_template = None

    def __init__(self, job_config, executor, path):
        super(TemplateMixin, self).__init__(job_config, executor, path)

        template_loader = jinja2.ChoiceLoader([
            jinja2.FileSystemLoader(os.getcwd()),
            jinja2.PackageLoader('kronos_executor', 'job_templates')
        ])
        self.template_env = jinja2.Environment(loader=template_loader)

        self.job_template_name = job_config.get("job_template", self.default_template)
        if self.job_template_name is None:
            raise ValueError(
                "No default template for job class {} and no 'job_template' specified".format(job_config['job_class']))

    def generate_script(self, script_format):
        template = self.template_env.get_template(self.job_template_name)
        stream = template.stream(script_format)
        with open(self.submit_script, 'w') as f:
            stream.dump(f)

class TemplateJob(TemplateMixin, UserAppJob):
    pass
