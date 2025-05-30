import datetime
import heapq

# ---- Shift Configuration ----
SHIFT_TIMES = {
    1: (datetime.time(8, 0), datetime.time(16, 30)),   # Shift 1: 8:00 AM - 4:30 PM (8.5 hrs)
    2: (datetime.time(16, 30), datetime.time(1, 30)),  # Shift 2: 4:30 PM - 1:30 AM (9 hrs, overnight)
    3: (datetime.time(1, 30), datetime.time(8, 0)),    # Shift 3: 1:30 AM - 8:00 AM (6.5 hrs, overnight)
}

def is_sunday(dt):
    return dt.weekday() == 6

def next_working_day(dt):
    next_day = dt + datetime.timedelta(days=1)
    while is_sunday(next_day):
        next_day += datetime.timedelta(days=1)
    return next_day

def get_shift_for_time(time):
    for shift, (start, end) in SHIFT_TIMES.items():
        if start < end:
            if start <= time < end:
                return shift
        else:  # Overnight shift
            if time >= start or time < end:
                return shift
    return None

def get_next_shift_start(resource_shifts, current_dt):
    sorted_shifts = sorted(SHIFT_TIMES.items(), key=lambda x: x[1][0])
    for days_ahead in range(0, 8):  # Check next 7 days
        day = current_dt.date() + datetime.timedelta(days=days_ahead)
        if is_sunday(day):
            continue
        for shift, (start_time, end_time) in sorted_shifts:
            if shift not in resource_shifts:
                continue
            shift_start = datetime.datetime.combine(day, start_time)
            if start_time > end_time and days_ahead == 0:
                if current_dt.time() > start_time:
                    shift_start += datetime.timedelta(days=1)
            if shift_start > current_dt:
                return shift_start
    raise ValueError("No valid shift found in the next 7 days")

def add_hours_across_shifts(start_dt, hours, resource_shifts):
    dt = start_dt
    hours_left = hours
    while hours_left > 0:
        if is_sunday(dt):
            dt = next_working_day(dt).replace(hour=0, minute=0)
            continue
        shift = get_shift_for_time(dt.time())
        if shift not in resource_shifts:
            dt = get_next_shift_start(resource_shifts, dt)
            continue
        shift_end = datetime.datetime.combine(dt.date(), SHIFT_TIMES[shift][1])
        if SHIFT_TIMES[shift][0] > SHIFT_TIMES[shift][1]:  # Overnight
            shift_end += datetime.timedelta(days=1)
        available_time = (shift_end - dt).total_seconds() / 3600
        if available_time <= 0:
            dt = get_next_shift_start(resource_shifts, dt)
            continue
        if hours_left <= available_time:
            dt += datetime.timedelta(hours=hours_left)
            hours_left = 0
        else:
            dt = shift_end
            hours_left -= available_time
    return dt

class Resource:
    def __init__(self, name, operational_shifts, is_machine):
        self.name = name
        self.operational_shifts = operational_shifts
        self.is_machine = is_machine
        self.schedule = []  # (start, end, product_name, op_name)
    def is_available(self, start, end):
        for s, e, _, _ in self.schedule:
            if not (end <= s or start >= e):
                return False
        return True
    def add_operation(self, start, end, product_name, operation):
        self.schedule.append((start, end, product_name, operation))
        self.schedule.sort()
    def get_first_shift_start(self):
        if not self.schedule:
            return None
        first_op_start = self.schedule[0][0]
        for days_back in range(0, 7):
            day = first_op_start.date() - datetime.timedelta(days=days_back)
            if is_sunday(day):
                continue
            for shift in sorted(self.operational_shifts):
                shift_start = datetime.datetime.combine(day, SHIFT_TIMES[shift][0])
                if shift_start <= first_op_start:
                    return shift_start
        return None
    def get_last_shift_end(self):
        if not self.schedule:
            return None
        last_op_end = self.schedule[-1][1]
        for days_ahead in range(0, 7):
            day = last_op_end.date() + datetime.timedelta(days=days_ahead)
            if is_sunday(day):
                continue
            for shift in sorted(self.operational_shifts, reverse=True):
                shift_end = datetime.datetime.combine(day, SHIFT_TIMES[shift][1])
                if SHIFT_TIMES[shift][0] > SHIFT_TIMES[shift][1]:
                    shift_end += datetime.timedelta(days=1)
                if shift_end >= last_op_end:
                    return shift_end
        return None

class Project:
    def __init__(self, data):
        self.product_name = data['product_name']
        self.pgma = data['pgma']
        self.du = data['du']
        self.priority = data['priority']
        self.start_time = datetime.datetime.strptime(
            f"{data['start_date']} {int(data['start_time']):02d}:00",
            "%Y-%m-%d %H:%M"
        )
        self.operations = data['operations']
        self.operation_sequence = data['operation_sequence']
        self.operation_times = data['operation_times']
        self.current_op = 0
        self.completion_time = None

class Scheduler:
    def __init__(self, machines, work_centers, projects):
        self.machine_names = set(m['machine_name'] for m in machines)
        self.work_center_names = set(wc['work_center'] for wc in work_centers)
        self.resources = {}
        for m in machines:
            self.resources[m['machine_name']] = Resource(m['machine_name'], m['operational_shifts'], True)
        for wc in work_centers:
            self.resources[wc['work_center']] = Resource(wc['work_center'], wc['operational_shifts'], False)
        self.projects = [Project(p) for p in sorted(projects, key=lambda x: x['priority'])]
        self.event_queue = []
        self.counter = 0

    def run(self):
        for project in self.projects:
            self.schedule_operation(project, project.start_time)
        while self.event_queue:
            time, _, project, op_idx = heapq.heappop(self.event_queue)
            if op_idx >= len(project.operations):
                continue
            op_name = project.operations[op_idx]
            resource_name = project.operation_sequence[op_idx]
            duration = project.operation_times[op_idx]
            resource = self.resources[resource_name]
            start = self.find_earliest_slot(resource, time, duration)
            end = add_hours_across_shifts(start, duration, resource.operational_shifts)
            resource.add_operation(start, end, project.product_name, op_name)
            project.current_op += 1
            if project.current_op < len(project.operations):
                self.schedule_operation(project, end)
            else:
                project.completion_time = end

    def find_earliest_slot(self, resource, ready_time, duration):
        candidate_start = ready_time
        while True:
            if is_sunday(candidate_start):
                candidate_start = next_working_day(candidate_start).replace(hour=0, minute=0)
                continue
            latest_end = candidate_start
            for s, e, _, _ in sorted(resource.schedule):
                if latest_end < s:
                    break
                if s <= latest_end < e:
                    latest_end = e
            candidate_start = latest_end
            while True:
                shift = get_shift_for_time(candidate_start.time())
                if shift in resource.operational_shifts and not is_sunday(candidate_start):
                    break
                candidate_start = get_next_shift_start(resource.operational_shifts, candidate_start)
            candidate_end = add_hours_across_shifts(candidate_start, duration, resource.operational_shifts)
            if resource.is_available(candidate_start, candidate_end):
                return candidate_start
            candidate_start = candidate_end

    def schedule_operation(self, project, start_time):
        self.counter += 1
        heapq.heappush(self.event_queue, (start_time, self.counter, project, project.current_op))

    def calculate_idle_times(self):
        idle_times = {}
        for res_name, resource in self.resources.items():
            if not resource.schedule:
                idle_times[res_name] = 0.0
                continue
            idle = 0.0
            intervals = [(s, e) for s, e, _, _ in sorted(resource.schedule)]
            first_shift_start = resource.get_first_shift_start()
            last_shift_end = resource.get_last_shift_end()
            if not first_shift_start or not last_shift_end:
                idle_times[res_name] = 0.0
                continue
            current = first_shift_start
            while current < last_shift_end:
                if is_sunday(current):
                    current = next_working_day(current).replace(hour=0, minute=0)
                    continue
                for shift in resource.operational_shifts:
                    shift_start = datetime.datetime.combine(current.date(), SHIFT_TIMES[shift][0])
                    shift_end = datetime.datetime.combine(current.date(), SHIFT_TIMES[shift][1])
                    if SHIFT_TIMES[shift][0] > SHIFT_TIMES[shift][1]:
                        shift_end += datetime.timedelta(days=1)
                    shift_idle = (shift_end - shift_start).total_seconds() / 3600.0
                    for s, e in intervals:
                        overlap_start = max(shift_start, s)
                        overlap_end = min(shift_end, e)
                        if overlap_start < overlap_end:
                            shift_idle -= (overlap_end - overlap_start).total_seconds() / 3600.0
                    if shift_idle > 0:
                        idle += shift_idle
                current += datetime.timedelta(days=1)
            idle_times[res_name] = round(idle, 2)
        return idle_times

def print_schedule(scheduler):
    print("=== PROJECT SCHEDULE VIEW ===")
    print("-" * 130)
    print(f"{'Product':15} {'PGMA':10} {'DU':10} {'Operation':15} {'Resource':10} {'Start':20} {'End':20} {'Queue Hrs':9}")
    print("-" * 130)
    for project in scheduler.projects:
        prev_end = project.start_time
        for op_idx, op_name in enumerate(project.operations):
            resource_name = project.operation_sequence[op_idx]
            found = None
            for s, e, prod_name, op in scheduler.resources[resource_name].schedule:
                if prod_name == project.product_name and op == op_name:
                    found = (s, e)
                    break
            if found:
                start, end = found
                queue_hrs = max(0.0, (start - prev_end).total_seconds() / 3600)
                print(
                    f"{project.product_name:15} {project.pgma:10} {project.du:10} "
                    f"{op_name:15} {resource_name:10} {start.strftime('%Y-%m-%d %H:%M'):20} "
                    f"{end.strftime('%Y-%m-%d %H:%M'):20} {queue_hrs:9.1f}"
                )
                prev_end = end
        print(f"{project.product_name} COMPLETES: {project.completion_time.strftime('%Y-%m-%d %H:%M')}")
        print("-" * 130)

    print("\n=== RESOURCE SCHEDULE VIEW ===\n")
    for resource_name in sorted(scheduler.resources.keys()):
        resource = scheduler.resources[resource_name]
        print(f"Resource {resource_name} Schedule:")
        print("-" * 110)
        print(f"{'Product':15} {'PGMA':10} {'DU':10} {'Operation':15} {'Start':20} {'End':20} {'Duration':8}")
        print("-" * 110)
        for s, e, prod_name, op in sorted(resource.schedule):
            dur = (e - s).total_seconds() / 3600.0
            # Find project details from product_name (assuming unique product_name)
            project = next(
                (proj for proj in scheduler.projects if proj.product_name == prod_name),
                None
            )
            pgma = project.pgma if project else "N/A"
            du = project.du if project else "N/A"
            print(
                f"{prod_name:15} {pgma:10} {du:10} {op:15} {s.strftime('%Y-%m-%d %H:%M'):20} "
                f"{e.strftime('%Y-%m-%d %H:%M'):20} {dur:8.1f} hrs"
            )
        print()
    idle_times = scheduler.calculate_idle_times()
    print("=== RESOURCE IDLE TIMES ===")
    for resource_name in sorted(idle_times.keys()):
        print(f"{resource_name:10}: {idle_times[resource_name]:.1f} hrs idle")

# ---- Example Usage ----
if __name__ == "__main__":
    machines = [
        {"machine_name": "M1", "operational_shifts": [1,2]},
        {"machine_name": "M2", "operational_shifts": [1,2]},
        {"machine_name": "M3", "operational_shifts": [1,2]},
        {"machine_name": "M4", "operational_shifts": [1,2]},
        {"machine_name": "M5", "operational_shifts": [1,2]}
    ]
    work_centers = [
        {"work_center": "W1", "Fitters": 2, "Welders": 3, "operational_shifts": [1,2]},
        {"work_center": "W2", "Fitters": 4, "Welders": 3, "operational_shifts": [1]},
        {"work_center": "W3", "Fitters": 2, "Welders": 5, "operational_shifts": [1,2,3]},
        {"work_center": "W4", "Fitters": 1, "Welders": 3, "operational_shifts": [1,2]},
        {"work_center": "W5", "Fitters": 2, "Welders": 2, "operational_shifts": [1,2,3]}
    ]
    projects = [
        {
            "product_name": "Engine Block V6",
            "pgma": "PGMA-123",
            "du": "DU-456",
            "priority": 1,
            "start_date": "2025-04-15",
            "start_time": 8.0,
            "operations": ["Cutting", "Welding", "Assembly", "Drilling", "Washing", "Painting", "Fitting"],
            "operation_sequence": ["M1", "M3", "M5", "M2", "M3", "M1", "W1"],
            "operation_times": [9,5,9,9,2,6,11]
        },
        {
            "product_name": "Turbocharger T7",
            "pgma": "PGMA-789",
            "du": "DU-012",
            "priority": 2,
            "start_date": "2025-04-15",
            "start_time": 8.0,
            "operations": ["Cutting", "Dusting", "Welding", "Assembly", "Galvanizing", "Moulding", "Fitting", "Repainting"],
            "operation_sequence": ["M2", "M4", "W3", "M5", "M1", "M2", "M3", "W2"],
            "operation_times": [2,9,2,2,9,6,9,9]
        }
    ]
    scheduler = Scheduler(machines, work_centers, projects)
    scheduler.run()
    print_schedule(scheduler)
