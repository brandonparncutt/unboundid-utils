#!/usr/bin/env python
# manage_backups.py


"""
manage_backups.py:  script to manage Unbound ID (now known as Ping ID) backups
and backup operations, including restores and rotation. It can be executed on
the command-line / crontab, and includes an option to install itself in cron.
"""


import time
import sys
import os
import re
import smtplib
import shutil
import glob
import datetime
from subprocess import Popen, PIPE
from optparse import OptionParser, OptionGroup


# Timestamp format for log file
timestamp = '[' + time.strftime('%Y/%m/%d - %H:%M:%S') + ']  '
# Email recipients for notifications
toaddr = ('brandon.parncutt@techdata.com',)
# Sending address
fromaddr = '%s@somedomain.com' % os.uname()[1]
# relay server
mail_server = 'localhost'
mail_server_port = 25


def send_email(body=None):
    emailout = Logger()
    subject_header = 'Subject: Backup Notification -- %s' % (
                                    time.strftime("%a, %d %b %Y %H:%M:%S %p"))
    from_header = "From: %s\r\n" % fromaddr
    to_header = "To: %s\r\n\r\n" % toaddr
    body = body
    email_message = ("%s\n\n%s" % (subject_header, body))
    sendit = smtplib.SMTP(mail_server, mail_server_port)
    try:
        sendit.sendmail(fromaddr, toaddr, email_message)
    except smtplib.SMTPException as emailerr:
        errmsg = (sys.exc_info()[0], sys.exc_info()[1])
        for part in errmsg:
            emailout.write(part)
    finally:
        sendit.quit()
    emailout.write("Email sent to %s" % toaddr)


class Logger(object):
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open(options.logfile, "a+")

    def write(self, message):
        self.terminal.write(message + '\n')
        self.log.write(timestamp + message + '\n')

    def flush(self):
        self.log.flush()


class BackupActions(object):
    """ enumerate and rotate backups in path, according to preset config """

    def __init__(self, incremental=False):
        self.out = Logger()
        self.hourly = incremental

    @staticmethod
    def get_size(path):
        total_size = 0
        seen = {}
        for dirpath, dirnames, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                try:
                    stat = os.lstat(fp)
                except OSError:
                    continue
                try:
                    seen[stat.st_ino]
                except KeyError:
                    seen[stat.st_ino] = True
                else:
                    continue
                total_size += stat.st_size
        return total_size

    @staticmethod
    def enumerateBackups(path):
        """
        Returns all backups under path, recursively. Backups will be listed by:
        [
        backend name, directory name...and a tuple of:
        (backupid, backupdate, incremental boolean)
        ]
        """
        backuplist = []
        # Regex matches for backup info from 'backup.info' file(s):
        backendregex = re.compile('(?<=ds-cfg-backend-id=)\w+')
        backupregexes = re.compile(r'''
                                   (?:^backup_id=)(\w+)$
                                   (?:\n)
                                   (?:^backup_date=)(\w+)
                                   (?:\n)
                                   (?:^incremental=)(true|false)''',
                                   re.VERBOSE | re.MULTILINE)
        for (dirname, dirshere, fileshere) in os.walk(path):
            for filename in fileshere:
                backend = []
                matches = []
                if filename == 'backup.info':
                    backupfile = os.path.join(dirname, filename)
                    try:
                        file = open(backupfile)
                        backuptext = file.read()
                    except IOError:
                        continue
                    finally:
                        file.flush()
                        file.close()
                    backend = backendregex.search(backuptext).group(0)
                    matches = backupregexes.findall(backuptext)
                    backups = [backend, dirname, matches]
                    backuplist.append(backups)
        return backuplist

    def rotate(self):
        with open(os.environ['HOME'] + '/.backup_config') as config:
            path = config.readline().rstrip()
            freespace = int(config.readline().rstrip())
            maxarchives = int(config.readline().rstrip())
            try:
                freebytes = int(Cron.analyze(path, freespace))
                # logit = BackupActions(path)
            except IOError:
                msg = ('\n!! Data Store backup failure !!\n\n'
                       'There is not enough free space to continue.'
                       ' Please cleanup ' + path + ' manually.\n\n')
                send_email(body=msg + "Sincerely,\n\nYour faithful program")
                self.out.write(msg)
                sys.exit(1)
            mfree = freebytes / (1024 ** 2)
            self.out.write("Checking size...There are " + str(mfree) +
                           "MB free...continuing...")
            backuplist = BackupActions.enumerateBackups(path)
            # Build a list of backup IDs (each backend can share a backup ID)
            backupids = list(set([backup[2][0][0] for backup in backuplist]))
            # Build a list of backup folders (the object of file operations)
            backupfolders = sorted(
                list(set(
                    [os.path.split(backup[1])[:-1][0] for backup in
                     backuplist])))
            excludelist = ('daily', 'weekly', 'today', 'yearly', 'monthly')
            backupfolders[:] = [
                d for d in backupfolders if not d.endswith(excludelist)]
            count = len(backupfolders)
            today = datetime.date.today()
            tomorrow = datetime.date.today() + datetime.timedelta(1)
            yesterday = datetime.date.today() - datetime.timedelta(1)
            yesterdir = yesterday.strftime('%Y%m%d')
            weekdaynum = datetime.datetime.isoweekday(today)
            weeknum = today.strftime('%U')
            if not self.hourly:
                old = glob.glob(path + '/today/*')
                for backend in old:
                    shutil.move(backend, path + '/daily/' + yesterdir)
                if datetime.datetime.isoweekday(yesterday) == 6:
                    weeklydest = yesterday.strftime('%Y%m%d-%U')
                    shutil.copytree(path + '/daily/' + yesterdir,
                                    path + '/weekly/' + weeklydest)
                if yesterday.strftime('%m') != today.strftime('%m'):
                    shutil.copytree(path + '/daily/' + yesterdir,
                                    path + '/monthly/' + yesterday.strftime(
                                        '%Y%m'))
                if yesterday.strftime('%m%d') == '1231':
                    year = yesterday.strftime('%Y')
                    shutil.copytree(path + '/daily/' + yesterdir,
                                    path + '/yearly/' + year)
                dailyglob = glob.glob(path + '/daily/*')
                for folder in dailyglob:
                    if datetime.datetime.isoweekday(
                        datetime.datetime.strptime(os.path.basename(folder),
                                                   '%Y%m%d')) == weekdaynum:
                        shutil.rmtree(folder, ignore_errors=True)
                weeklyglob = glob.glob(path + '/weekly/*')
                if weekdaynum == 6:
                    for folder in weeklyglob:
                        if os.path.basename(folder).endswith(weeknum):
                            shutil.rmtree(folder, ignore_errors=True)
                if today.strftime('%m') != tomorrow.strftime('%m'):
                    shutil.rmtree(path + '/monthly/' + today.strftime('%m'))
            if count > maxarchives:
                difference = count - maxarchives
                deletelist = backupfolders[:difference]
                for folder in deletelist:
                    shutil.rmtree(folder[0], ignore_errors=True)


class Backup(object):
    """
    backup datastore depending on options and save output / errors for logs or
    email notifications
    """

    def __init__(self, command, options):
        self.options = {k: options[k] for k in options if options[k] is not
                        None}
        self.logfile = self.options.pop('logfile')
        self.datestart = time.strftime("%a, %d %b %Y %H:%M:%S %p")
        self.timestart = time.time()
        self.command = command
        try:
            self.log = open(self.logfile, 'a')
            self.log.write(sepline)
            self.log.write("%s beginning at %s\n" % (self.command.capitalize(),
                                                     self.datestart))
        except IOError:
            print("Unable to create log file in %s...please check."
                  % self.logfile)
        finally:
            self.log.close()

    def parse_args(self, **args):
        pargs = ['--{0}'.format(k) for k in args.keys() if args[k] is True]
        for key in (k for k in args.keys() if args[k] is True):
            del args[key]
        kargs = []
        for key, value in args.items():
            if isinstance(value, list):
                val = ' '.join(value)
            else:
                val = value
            kargs.append("--{0}".format(key))
            kargs.append("{0}".format(value))
        args = pargs + kargs
        args.insert(0, self.command)
        self.sysout = Logger()
        argsmessage = "Arguments passed to utility:"
        self.sysout.write(argsmessage)
        for arg in args[1:]:
            self.sysout.write(arg)
        return args

    def run(self):
        cmdargs = self.parse_args(**self.options)
        cmd = Popen(cmdargs, stdout=PIPE, stderr=PIPE,
                        universal_newlines=True)
        cmdstdout, cmdstderr = cmd.communicate()
        if cmd.returncode != 0:
            errormsg = ("\n'%s':\n\n\nOUTPUT:\n%s\nERROR:\n%s" %
                        (' '.join(cmdargs), cmdstdout, cmdstderr))
            self.sysout.write(str(errormsg))
            send_email(body=errormsg)
            raise RuntimeError(errormsg)
        self.sysout.write(str(cmdstdout))
        self.dateend = time.strftime("%a, %d %b %Y %H:%M:%S %p")
        self.timeend = time.time()
        self.sysout.write(sepline)
        self.sysout.write("%s completed at %s\tTotal elapsed time: %.2f sec"
                          % (self.command.capitalize(), self.dateend,
                             (self.timeend - self.timestart)))


class Cron(object):
    """
    Take scheduled backups and rotate them as neeeded.
    """

    def __init__(self, options):
        self.cronout = Logger()
        self.options = { k: options[k] for k in options if options[k] != None }
        self.freespace = int(self.options['freespace'][:-1])
        if self.options['freespace'].endswith('M'):
            self.freespace *= (1024 ** 2)
        if self.options['freespace'].endswith('G'):
            self.freespace *= (1024 ** 3)
        self.path = os.path.abspath(self.options['backupDirectory'])
        try:
            self.maxarchives = int(self.options['maxbackups'])
        except KeyError:
            self.maxarchives = 10
        homefolder = os.environ['HOME']
        with open(homefolder + '/.backup_config', 'w') as configfile:
            configfile.write(self.path + "\n")
            configfile.write(str(self.freespace) + "\n")
            configfile.write(str(self.maxarchives) + "\n")
        if 'setupcron' in self.options:
            self.cronopts = self.options['setupcron']
            try:
                from crontab import CronTab
            except ImportError:
                errmsg = ("Module needed...please install the 'python-crontab'"
                          " => 'pip intall python-crontab'", sys.exc_info()[3])
                self.cronout.write(errmsg)
                sys.exit(1)
            finally:
                class crontab(CronTab):
                    """
                    this is a wrapper class to circumvent a bug in the crontab
                    module which causes an error.
                    """
                    def __init__(self, **kargs):
                        import logging
                        LOG = logging.getLogger(
                            'crontab').addHandler(logging.NullHandler())
                        CronTab.__init__(self, **kargs)
                self.cron = crontab(user=True)

    def checkCrontab(self, args):
        if 'daily' in args:
            daily = True
        else:
            daily = False
        if 'hourly' in args:
            hourly = True
        else:
            hourly = False
        tab = self.cron.crons[:]
        hourlyindex = -1
        dailyindex = -1
        for index in range(len(tab)):
            if re.search('DS\ BACKUP-HOURLY', str(tab[index])):
                hourlyindex = index
            if re.search('DS\ BACKUP-DAILY', str(tab[index])):
                dailyindex = index
        if hourlyindex >= 0:
            self.cron.remove(self.cron.crons[hourlyindex])
        if dailyindex >= 0:
            self.cron.remove(self.cron.crons[dailyindex])
        crons = (daily, hourly)
        return crons

    @staticmethod
    def analyze(path, freespace):
        used = BackupActions.get_size(path)
        wanted = freespace
        fs = os.statvfs(path)
        actual = fs.f_frsize * (fs.f_bfree * .95)  # Account for reserved %
        if not wanted <= actual:
            with open(options.logfile, 'a+') as log:
                log.write(timestamp + 'The minimum space specified is not ' +
                          'available...cannot continue.' + '\n')
            raise IOError('The minimum space specified is not available...'
                          'cannot continue.' + '\n')
        else:
            return actual

    def run(self):
        daily, hourly = self.checkCrontab(self.cronopts)
        backupidstring = r'"$(date +\%a-\%Y\%m\%d)"'
        hourlybackupidstring = r'"$(date +\%a-\%Y\%m\%d--\%H)"'
        backupdir = self.path + '/today/'
        dailycmd = ('manage_backups.py backup -a -c -d %s -I %s' % (
            backupdir, backupidstring))
        hourlycmd = (
            'manage_backups.py backup -a -c -d %s -B %s -i -I %s' % (
                backupdir, backupidstring, hourlybackupidstring))
        if daily:
            dailyjob = self.cron.new(command=dailycmd)
            dailyjob.hour.on(00)
            dailyjob.set_comment("DS BACKUP-DAILY")
        if hourly:
            hourlyjob = self.cron.new(command=hourlycmd)
            hourlyjob.minute.on(30)
            hourlyjob.set_comment("DS BACKUP-HOURLY")
        self.cron.env['PATH'] = os.environ['PATH']
        self.cron.write()
        if not os.path.exists(self.path):
            try:
                os.mkdir(self.path)
            except IOError:
                errmsg = ('Error creating folder: %s, %s' %
                          (os.path.join(self.path, dir), sys.exc_info()[1]))
                self.cronout.write(errmsg)
                pass
        for dir in ['today', 'daily', 'weekly', 'monthly', 'yearly']:
            if not os.path.exists(os.path.join(self.path, dir)):
                try:
                    os.mkdir(self.path + '/' + dir)
                except IOError:
                    errmsg = ('Error creating folder: %s, %s' %
                              (os.path.join(self.path, dir),
                              sys.exc_info()[1]))
                    self.cronout.write(errmsg)
                    continue
        try:
            Cron.analyze(self.path, self.freespace)
        except IOError:
            errmsg = ('The current setting for minimum "--free-space" is '
                      'not possible. The backup scripts will not run'
                      ' until this has been rectified '
                      '(remove files, increase setting, etc.).')
            self.cronout.write(errmsg)
        else:
            backuppath = self.path + '/today'
            backupid = datetime.date.today().strftime('%a-%Y%m%d')
            backupdict = {'backUpAll': True, 'compress': True,
                          'backupDirectory': backuppath,
                          'backupID': backupid,
                          'logfile': options.logfile}
            print('Creating first backup for crons...')
            time.sleep(3)
            cronBackup = Backup('backup', backupdict)
            cronBackup.run()


class Restore(Backup):
    """
    restore from specified backup
    """


if __name__ == '__main__':
    sepline = "-" * 80 + "\n"
    usage = '''

%prog command [options]

'command' is one of the following:
    - backup
    - restore
    - setup-cron (recommended backup method)
'''
    version = '%prog:  1.1'
    parser = OptionParser(usage=usage, version=version)
    parser.add_option("-I", "--backupID", action="store", type="string",
                      dest="backupID", help="?? -- the completed backup name")
    parser.add_option("-d", "--backupDirectory", action="store", type="string",
                      dest="backupDirectory",
                      help="?? -- the backups folder")
    parser.add_option("-L", "--log-to-file", action="store", type="string",
                      help="?? -- log to /path/to/log/file for posterity",
                      default=(os.environ['HOME'] + '/backup.log'),
                      dest="logfile")

    backuphelp = OptionGroup(parser, "Backup Options",
                            "These options are only compatible with the "
                            "'backup' command.")
    backuphelp.add_option("-a", "--backUpAll", action="store_true",
                      dest="backUpAll",
                      help="?? -- backup all backends")
    backuphelp.add_option("-i", "--incremental", action="store_true",
                      dest="incremental",
                      help="?? -- perform incremental from previous backup[-B]",
                      )
    backuphelp.add_option("-B", "--incrementalBaseID", action="store",
                      type="string", dest="incrementalBaseID",
                      help="?? -- the ID of original backup to increment")
    backuphelp.add_option("-c", "--compress", action="store_true",
                          dest="compress", help="?? -- compresses the backup "
                          "when flag is present")
    backuphelp.add_option("-n", "--backendID", action="append", type="string",
                      dest="backendID", help="?? -- backup just this backend")
    parser.add_option_group(backuphelp)

    restorehelp = OptionGroup(parser, "Restore Options",
                              "These options are only compatible with "
                              "the 'restore' command.")
    restorehelp.add_option("-l", "--listBackups", action="store_true",
                           help="lists backups in designated location [-d]")
    restorehelp.add_option("-N", "--dry-run", action="store_true",
                           help="verify contents of backup but don't restore",
                           dest="dry-run")
    parser.add_option_group(restorehelp)

    cronhelp = OptionGroup(parser, "Cron-mode Options",
                             "These options are only to be used with "
                             "the 'cron-mode' command. When run in this mode "
                             "backups are automatic: daily (up to --max), and "
                             "hourly (for the userRoot backend). It will handle"
                             " backup rotation, minding the 'freespace' option."
                             " Entries will need to be added in crontab "
                             "to begin using, with '--setup [daily|hourly]'.")
    cronhelp.add_option("-f", "--free-space", action="store",
                           dest="freespace",
                           help="?? -- Amount of space to keep free on backup "
                           "partition. Units: [M]egabytes, [G]igabytes",
                           metavar="FREESPACE[MG]")
    cronhelp.add_option("-m", "--max", action="store", dest="maxbackups",
                        help="?? -- maximum number of backups to keep",
                        metavar="NUM")
    cronhelp.add_option("-s", "--setup", action="append", dest="setupcron",
                        help="?? -- adds entries to user's crontab for "
                        "[hourly/daily] backup jobs", metavar="hourly daily")
    parser.add_option_group(cronhelp)

    (options, args) = parser.parse_args()
    if not args:
        parser.print_help()
    if options.backUpAll and options.backendID:
        parser.error("Options '--%s' and '--%s' are mutually exclusive" % (
                     'backUpAll', 'backendID'))
    if options.incremental:
        if not options.incrementalBaseID:
            parser.error("A base ID for original backup [-B] must be" +
            " specified for an incremental backup.")
    if 'backup' in args:
        if not options.backupDirectory:
            parser.error("The destination for backup must be specified.")
        backupoptions = options.__dict__
        if not sys.stdout.isatty():
            if options.incremental:
                cronbackup = BackupActions(incremental=True)
                try:
                    cronbackup.rotate()
                except:
                    msg = ("!! Data Store Backup Problem !!\n\nThere was an "
                           "issue with rotating backups:\n\n%s, %s" %
                           sys.exc_info()[0], sys.exc_info()[1])
                    send_email(body=msg)
            else:
                cronbackup = BackupActions()
                try:
                    cronbackup.rotate()
                except:
                    msg = ("!! Data Store Backup Problem !!\n\nThere was an "
                           "issue with rotating files:\n\n%s %s" %
                           (sys.exc_info()[0], sys.exc_info()[1]))
                    send_email(body=msg)
        newbackup = Backup('backup', backupoptions)
        newbackup.run()
    elif 'restore' in args:
        if not options.backupDirectory:
            parser.error("Path to the directory containing the backups must"
                         " be specified.")
        restoreoptions = options.__dict__
        startrestore = Restore('restore', restoreoptions)
        startrestore.run()
    elif 'setup-cron' in args:
        if not options.backupDirectory:
            parser.error("Path to backups folder must be specified.")
        if not options.freespace:
            pathstat = os.stat(options.backupDirectory)
            rootstat = os.stat('/')
            if pathstat.st_dev == rootstat.st_dev:
                parser.print_help()
                parser.error("'freespace' option must be specified when backups"
                             " folder shares a partition with the OS to prevent"
                             " system failure...\nBe sure to include enough "
                             "space for normal system functionality and logs.")
            else:
                options.freespace = '200M'
        else:
            regex = re.compile('(\d+[M|G])')
            options.freespace = options.freespace.upper()
            try:
                result = regex.match(options.freespace)
                options.freespace = result.group(0)
            except:
                parser.print_help()
                parser.error("Invalid syntax:\nPlease correct the 'freespace'"
                             " option and try again.")
        setcron = Cron(options.__dict__)
        setcron.run()
    elif 'menu' in args:
        print("This feature will be coming very soon!")
    else:
        print("\n\nCome on, fat fingers!!! Check your command!\n\n")
