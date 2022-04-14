import io
import json
import couchdb

from params import COUCH_LINK


def init_couchdb_workload(couch, input_dict):
    for function_id in input_dict.keys():
        if function_id in couch:
            db = couch[function_id]
        else:
            db = couch.create(function_id)

        # Create input doc
        for input_name in input_dict[function_id].keys():
            path = input_dict[function_id][input_name]["path"]
            content_type = input_dict[function_id][input_name]["content_type"]

            # Create a doc for input attachment if not exist
            if input_name not in db:
                db.save(
                    {
                        "_id": input_name,
                        "function_id": function_id
                    }
                )

            # Put attachment
            db.put_attachment(
                doc=db[input_name],
                content=open(path, "rb"),
                filename=input_name,
                content_type=content_type
            )

        # Create output doc
        if "result" not in db:
            db.save(
                {
                    "_id": "result",
                    "function_id": function_id
                }
            )


if __name__ == "__main__":
    # Set up connection to CouchDB
    couch = couchdb.Server(COUCH_LINK)

    # Init input dict
    input_dict = {
        "dh": {
            "template.html": {
                "path": "../workloads/python_dynamic_html/input/template.html",
                "content_type": "text/html"
            }
        },
        "eg": {},
        "ip": {
            "the_screamer_yui.png": {
                "path": "../workloads/python_image_processing/input/the_screamer_yui.png",
                "content_type": "image/png"
            }
        },
        "vp": {
            "watermark.png": {
                "path": "../workloads/python_video_processing/input/watermark.png",
                "content_type": "image/png"
            },
            "hi_chitanda_eru.mp4": {
                "path": "../workloads/python_video_processing/input/hi_chitanda_eru.mp4",
                "content_type": "video/mp4"
            }
        },
        "ir": {
            "tesla.jpg": {
                "path": "../workloads/python_image_recognition/input/tesla.jpg",
                "content_type": "image/jpeg"
            },
            "resnet50.pth": {
                "path": "../workloads/python_image_recognition/input/resnet50.pth",
                "content_type": None
            },
            "imagenet_class_index.json": {
                "path": "../workloads/python_image_recognition/input/imagenet_class_index.json",
                "content_type": "application/json"
            }
        },
        "knn": {},
        "alu": {},
        "ms": {},
        "gd": {},
        "dv": {
            "gene1.txt": {
                "path": "../workloads/python_dna_visualization/input/gene1.txt",
                "content_type": "text/plain"
            },
            "gene2.txt": {
                "path": "../workloads/python_dna_visualization/input/gene2.txt",
                "content_type": "text/plain"
            }
        }
    }

    # Init workload input
    init_couchdb_workload(couch, input_dict)

    print("")
    print("Workload input all set in CouchDB!")
    print("")
    