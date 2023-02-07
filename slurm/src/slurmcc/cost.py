import subprocess

def run_command(cmd: str, stdout=subprocess.PIPE, stderr=subprocess.PIPE):

    cmd_list = cmd.split(' ')

    log.debug(cmd_list)
    try:
        output = subprocess.run(cmd_list,stdout=stdout,stderr=stderr, check=True,
                       encoding='utf-8')
    except subprocess.CalledProcessError as e:
        log.error(f"cmd: {e.cmd}, rc: {e.returncode}")
        log.error(e.stderr)
        raise
    log.debug(f"output: {output.stdout}")
    return output

def get_sacct_fields():
    options = []
    cmd = "/usr/bin/sacct -e"
    out = run_command(cmd)
    for line in out.stdout.split('\n'):
        for opt in line.split():
            options.append(opt.lower())

    return options

class Formatter():

    def __init__(self):

        self.DEFAULT_ACCOUNTING_FORMAT = "jobid,user,account,cluster,partition,ncpus,nnodes,submit,start,end,elapsedraw,state,admincomment"
        self.DEFAULT_OUTPUT_FORMAT = "jobid,user,cluster,ncpus,nnodes,submit,start,end,elapsedraw,state,sku_name,region,meter,spot,cost,rate,currency"
        self.DEFAULT_AZCOST_FORMAT="sku_name,region,spot,meter,meterid,metercat,metersubcat,resourcegroup,cost,rate,currency"
        self.options = "--allusers --duplicates --parsable2 --allocations --noheader"
        self.slurm_avail_fmt = get_sacct_fields()
        self.az_fmt = [x.lower() for x in self.DEFAULT_AZCOST_FORMAT.split(',')]
        self.in_fmt = set(self.DEFAULT_ACCOUNTING_FORMAT.split(','))
        self.out_fmt = self.DEFAULT_OUTPUT_FORMAT.split(',')
        self.az_fmt_t = namedtuple('az_fmt', self.az_fmt)
        self.out_fmt_t = None
        self.pricing = namedtuple("pricing", "meter,meterid,metercat,metersubcat,resourcegroup,rate,cost,currency")

    def validate_format(self, ctx, fmt, value: str):

        add_fmt = False
        subtract_fmt = False
        if value.startswith("+"):
            value = value.lstrip("+")
            add_fmt = True
        elif value.startswith("-"):
            value = value.lstrip("-")
            subtract_fmt = True
        _value = [x.lower() for x in value.split(',')]
        wrong_params = [x for x in _value if x not in self.slurm_avail_fmt and x not in self.az_fmt]
        if wrong_params:
            raise click.BadParameter(f"These parameters are invalid: {' '.join(wrong_params)}")
        for f in _value:
            if f in self.slurm_avail_fmt:
                self.in_fmt.add(f)

        if add_fmt:
            [self.out_fmt.append(x) for x in _value if x not in self.out_fmt]
        elif subtract_fmt:
            [self.out_fmt.remove(x) for x in _value if x in self.out_fmt]
        else:
            self.out_fmt = _value

        self.out_fmt_t = namedtuple('out_fmt_t', self.out_fmt)
        self.in_fmt_t = namedtuple('in_fmt_t', self.in_fmt)
        return _value

    def get_slurm_format(self):

        return ','.join(self.in_fmt)

    def filter_by_slurm_user(self, ctx, fmt, value):

        if value:
            useropt = f"-u {value}"
            self.options = self.options + " " + useropt
        return value

    #TODO change this.
    def filter_by_partition(self, ctx, fmt, value):

        if value:
            opt = f"-r {value}"
            self.options = self.options + " " + opt
        return value
