from .mapreduce import run_mapreduce_job


def parse_row(row: str):
    return row.strip().split(",")


def mapper_add_tip_rate(sequence):
    result = []
    for idx, (_, row) in enumerate(sequence):
        if idx == 0:
            result.append((idx, row.strip() + ",tip_rate"))
        else:
            values = parse_row(row)
            total_bill, tip = float(values[0]), float(values[1])
            tip_rate = round(tip / total_bill, 2)
            result.append((idx, row.strip() + f",{tip_rate}"))
    return result


def mapper_filter_dinner(sequence):
    result = []
    for idx, (_, row) in enumerate(sequence):
        if idx == 0:
            result.append((idx, row.strip()))
        else:
            values = parse_row(row)
            if values[5] == "Dinner":
                result.append((idx, row.strip()))
    return result


def mapper_filter_dinner_tip_gt_5(sequence):
    result = []
    for idx, (_, row) in enumerate(sequence):
        if idx == 0:
            result.append((idx, row.strip()))
        else:
            values = parse_row(row)
            if values[5] == "Dinner" and float(values[1]) > 5.0:
                result.append((idx, row.strip()))
    return result


def mapper_filter_size_or_bill(sequence):
    result = []
    for idx, (_, row) in enumerate(sequence):
        if idx == 0:
            result.append((idx, row.strip()))
        else:
            values = parse_row(row)
            if int(values[6]) >= 5 or float(values[0]) > 45:
                result.append((idx, row.strip()))
    return result


def mapper_group_by_sex(sequence):
    result = []
    for idx, (_, row) in enumerate(sequence):
        if idx == 0:
            continue
        sex = parse_row(row)[2]
        result.append((sex, 1))
    return result


def reducer_identity(sequence):
    return sequence


def reducer_count(sequence):
    counts = {}
    for key, value in sequence:
        counts[key] = counts.get(key, 0) + value
    return list(counts.items())


def run():
    input_dir = "files/input"

    run_mapreduce_job(
        mapper=mapper_add_tip_rate,
        reducer=reducer_identity,
        input_directory=input_dir,
        output_directory="files/query_1",
    )

    run_mapreduce_job(
        mapper=mapper_filter_dinner,
        reducer=reducer_identity,
        input_directory=input_dir,
        output_directory="files/query_2",
    )

    run_mapreduce_job(
        mapper=mapper_filter_dinner_tip_gt_5,
        reducer=reducer_identity,
        input_directory=input_dir,
        output_directory="files/query_3",
    )

    run_mapreduce_job(
        mapper=mapper_filter_size_or_bill,
        reducer=reducer_identity,
        input_directory=input_dir,
        output_directory="files/query_4",
    )

    run_mapreduce_job(
        mapper=mapper_group_by_sex,
        reducer=reducer_count,
        input_directory=input_dir,
        output_directory="files/query_5",
    )


if __name__ == "__main__":
    run()
