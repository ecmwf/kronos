import json


def create_sa():

    out_folder = "/var/tmp/maab/iows/output/test_sa"
    
    kb_read_qty = 2.e6
    kb_write_qty = 2.e6
    flops_qty = 8.e9

    apps_data = [
        [[{"kb_write": kb_write_qty}], [{"flops": flops_qty}], [{"flops": flops_qty}], [{"flops": flops_qty}], [{"kb_read": kb_read_qty}]],
        [[{"kb_write": kb_write_qty}], [{"flops": flops_qty}], [{"flops": flops_qty}], [{"flops": flops_qty}], [{"kb_read": kb_read_qty}]],
        [[{"kb_write": kb_write_qty}], [{"flops": flops_qty}], [{"flops": flops_qty}], [{"flops": flops_qty}], [{"kb_read": kb_read_qty}]],

        [[{"kb_write": kb_write_qty}], [{"flops": flops_qty}], [{"kb_write": kb_write_qty}], [{"flops": flops_qty}], [{"kb_write": kb_write_qty}]],
        [[{"kb_write": kb_write_qty}], [{"flops": flops_qty}], [{"kb_write": kb_write_qty}], [{"flops": flops_qty}], [{"kb_write": kb_write_qty}]],
        [[{"kb_write": kb_write_qty}], [{"flops": flops_qty}], [{"kb_write": kb_write_qty}], [{"flops": flops_qty}], [{"kb_write": kb_write_qty}]],

        [[{"kb_read": kb_read_qty}], [{"flops": flops_qty}], [{"kb_read": kb_read_qty}], [{"flops": flops_qty}], [{"kb_read": kb_read_qty}]],
        [[{"kb_read": kb_read_qty}], [{"flops": flops_qty}], [{"kb_read": kb_read_qty}], [{"flops": flops_qty}], [{"kb_read": kb_read_qty}]],
        [[{"kb_read": kb_read_qty}], [{"flops": flops_qty}], [{"kb_read": kb_read_qty}], [{"flops": flops_qty}], [{"kb_read": kb_read_qty}]],
    ]

    for (aa, app_frames) in enumerate(apps_data):

        for frame in app_frames:
            if frame[0].has_key("kb_write"):
                frame[0]["mmap"] = False
                frame[0]["n_write"] = 1
                frame[0]["name"] = "file-write"
            if frame[0].has_key("kb_read"):
                frame[0]["mmap"] = False
                frame[0]["n_read"] = 1
                frame[0]["name"] = "file-read"
            if frame[0].has_key("flops"):
                frame[0]["name"] = "cpu"

        # - write output
        sa_output_data = {"num_procs": 2, "frames": app_frames}

        name_json_file_raw = out_folder + "/" + "job-"+str(aa)+".json"
        with open(name_json_file_raw, 'w') as f:
            json.encoder.FLOAT_REPR = lambda o: format(o, '.2f')
            json.dump(sa_output_data, f, ensure_ascii=True, sort_keys=True, indent=4, separators=(',', ': '))

if __name__ == '__main__':

    create_sa()
