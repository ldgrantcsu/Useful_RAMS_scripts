#! /bin/bash
##############################################################
# Program: x.repack.sh
# Programmer: Robert Seigel
#             University of Miami
#             rseigel@rsmas.miami.edu
#             16 March 2013
# Modified by: Leah Grant, May 2016
#
# Execute: x.repack.sh <jobname> [<dir>]
#          <jobname>: (MANDATORY) If using standard run type, then 
#                       <jobname> = rams executable name, e.g. rams-6.1.15
#                     If using LSF or Grid Engine scheduling, then 
#                       <jobname> = name of job
#                     If 2nd argument is input, <jobname> is not used but a dummy
#                       argument is still required
#                     *** Make sure "$exectype" is set correctly in this script! ***
#         [<dir>]: OPTIONAL 2rd argument. If RAMS has already finished,
#                    <dir> = the directory where output is located. 
#                    All RAMS h5 files in <dir> will be repacked.
#                    *** dummy argument 1 still required ***
#
# Purpose: This script repacks the HDF5 files while a distributed
#          memory RAMS run is occurring, or after a distributed 
#          memory RAMS run has finished. Because Parallel HDF5 is
#          not yet supporting online compression, this script 
#          takes its place to conserve disk space.
#
# Overview: This script runs while RAMS is running. It first looks
#           in RAMSIN to find where the files are being written.
#           Then, it iteratively checks to see if new files are
#           created. If a new file is output, it waits until the
#           file is created and then uses h5repack to repack
#           the HDF5 file. Once the RAMS processes are finished
#           and all HDF5 files have been repacked, x.repack.sh
#           will terminate.
# 
# Note: Check the ONLYEDIT THESE section and make sure you are happy
#       with the settings there!!!
#
# Examples: 
#  ./x.repack.sh rams-6.1.18_dm &> repack.out &         # for a standard run
#  ./x.repack.sh myLSFjob &> repack.out &               # for an LSF run
#  ./x.repack.sh dummy z.test01/NOBAK/ &> repack.out &  # for a run that's already done

##############################################################


##############################################################
# FUNCTION to find the new file and repack it according to 
# specified gzip level
repack ()
{
   # Loop over .h5 files in requested directory
   for f in $hfiles 
   do
      # search for # lines matching desired compression level in header information
      # if the # of lines = 0, the file is not compressed to the desired gzip level
      # finfo=$(h5dump -Hp $f | grep -ic LEVEL\ $gziplevel)

      hfile=${f:0:$((${#f}-5))}'head.txt'

      if [ $OnTheFly -eq 1 ]; then
         # Wait UNTIL header file exists to make sure HDF5 write is done
         until [ -e $hfile ]; do sleep 1; done

         # Case where file is being overwritten
         # check if this file needs to be repacked first, otherwise script will hang here
         # since, if the file was already repacked, the header file will be older
         finfo=$(h5dump -Hp $f | grep -ic LEVEL\ $gziplevel)
         if [ $finfo -eq 0 ]; then until [ ! $hfile -ot $f ]; do sleep 2; done; fi

         # In case the header file is big
         sleep 3
      fi

      finfo=$(h5dump -Hp $f | grep -ic LEVEL\ $gziplevel)
      if [ $finfo -eq 0 ]; then

         if [ -e $f.temp ]; then
            echo "$f is already being repacked. Skipping..."
         else
            # Repack the file
            echo "h5repack -f SHUF -f GZIP=$gziplevel $f $f.temp"
            h5repack -f SHUF -f GZIP=$gziplevel $f $f.temp
            mv $f.temp $f
            echo "New file $f has been repacked because it was not compressed to level $gziplevel"
         fi

	 if [ $scpfiles -eq 1 ]; then
	    # Now copy the file to a remote computer. RSA keys have already been generated.
            scp $f $copydir 
            scp $hfile $copydir
	    echo "$f and $hfile were copied to $copydir"
	 fi
      else
         echo "$f already repacked"
      fi

   done
}
######################################################################################
######################################################################################
# ONLYEDIT THESE

# Define some parameters
exectype=3         # Type of job execution. (Not used if 2nd argument is specified)
                   # 1 = Standard way, using a direct call to MPI
                   # 2 = LSF (Yellowstone)
                   # 3 = Grid Engine (snow cluster)
gziplevel=6        # gzip level - typically set to 6
checkint=5         # time controller in seconds
hfilestype="A"     # type of h5 files to repack: A for analysis only, L for lite only, AL for both
ramsin="RAMSIN"    # name of RAMSIN file
scpfiles=0         # 1=scp files, 0=do not scp files
if [ $scpfiles -eq 1 ]; then
   # Directory for file transfers
   # directory name is from character 5 onward in $1, since my job names on Yellowstone are "rams-$1"
   copydir='ldgrant@ccn.atmos.colostate.edu:/avalanche/ldgrant/RCE_YS/'${1:5:$((${#1}-5))}'/NOBAK/' 
   echo "copydir: $copydir"
fi

# END ONLYEDIT
######################################################################################
######################################################################################

# Perform some checks

# Check whether any arguments were specified or if first argument is help
if [[ -z "$1" || "$1" == "-h" || "$1" == "--help" ]]; then
   echo ""
   echo "Script usage:"
   echo " x.repack.sh <jobname> [<dir>]"
   echo "         <jobname>: [MANDATORY] If using standard run type, then "
   echo "                      <jobname> = rams executable name, e.g. rams-6.1.15 "
   echo "                    If using LSF or Grid Engine scheduling, then "
   echo "                      <jobname> = name of job"
   echo "                    If 2nd argument is input, <jobname> is not used but a dummy"
   echo "                      argument is still required"
   echo "                    *** Make sure \"\$exectype\" is set correctly in this script! ***"
   echo "         [<dir>]: OPTIONAL 2rd argument. If RAMS has already finished,"
   echo "                    <dir> = the directory where output is located."
   echo "                    All RAMS h5 files in <dir> will be repacked."
   echo "                    *** dummy argument 1 still required ***"
   echo ""
   echo "Examples:"
   echo " ./x.repack.sh rams-6.1.18_dm &> repack.out &         # for a standard run"
   echo " ./x.repack.sh myLSFjob &> repack.out &               # for an LSF run"
   echo " ./x.repack.sh dummy z.test01/NOBAK/ &> repack.out &  # for a run that's already done"
   echo ""
   exit
fi

# Is the user exercising the 2rd argument? 
if [ -z "$2" ]; then
# If not, check for RAMSIN
   OnTheFly=1
   RAMSINread=1
   if [ ! -f $ramsin ]; then
      echo "Cannot find RAMSIN file!"
      exit
   else
      echo "Looking in $ramsin for analysis file path"
   fi
   # set jobname from first command line input
   jobname=$1
else
# If $2 is not empty, don't read RAMSIN, and make sure there are h5 files present
   OnTheFly=0
   RAMSINread=0
   thisdir=$2/         # set directory to second argumemnt
   hfiles=$thisdir*-[$hfilestype]-*.h5 # look for analysis and/or lite files
   nhfiles=`ls -1 $hfiles | wc -l`
   if [ $nhfiles -eq 0 ]; then
      echo "No Analysis or Lite files in $thisdir!"
      exit
   fi
fi

##############################################################
# First Read RAMSIN to grab file locations

# This method is based on the fact that each RAMSIN parameter
# is preceeded by THREE SPACES. 
if [ $RAMSINread -eq 1 ]; then
   # ANALFILES
   # find the line
   thisdir=`egrep '   AFILEPREF' $ramsin`
   # remove suffix from 2nd single quote
   thisdir=${thisdir%\'*}
   # remove prefix from 1st single quote
   thisdir=${thisdir#*\'}
fi

##############################################################
# Execute repacking of HDF5 files

if [ $OnTheFly -eq 1 ]; then
  
   sleep $checkint
 
   # analysis files
   echo "Analysis files directory: $thisdir"
   # get files. pattern [AL]: look for analysis or lite files
   hfiles=$thisdir*-[$hfilestype]-*.h5 

   # Determine job string for repack execution
   if [ $exectype -eq 1 ]; then       # Standard
      jobout=$(pidof -s $jobname)
   elif [ $exectype -eq 2 ]; then     # LSF
      jobout=$(bjobs -l -J $jobname | grep -c $jobname)
      if [ $jobout -eq 0 ]; then jobout=""; fi
   elif [ $exectype -eq 3 ]; then     # Grid Engine
      #jobout=$(qstat -j $jobname | grep -c $jobname)
      jobout=$(qstat -f | grep -c $jobname)
   if [ $jobout -eq 0 ]; then jobout=""; fi
   fi
   echo "pid or jobname count: $jobout"

   # Initialize filecounts.
   # The repacking process can take a long time. During the repacking, files
   # may be generated and the loop in 'repack' does not see them. These vars
   # will test if new files have been created.
   prefilecount=`ls -1 $hfiles | wc -l` 
   postfilecount=0
   echo "prefilecount, postfilecount: $prefilecount $postfilecount"

   # Continue to execute repack if (1) the model is running OR 
   #                               (2) the number of files have changed since
   #                                   last repack, i.e. new files were generated
   while [[ -n "$jobout" ]] || [[ $prefilecount -ne $postfilecount ]]
   do   

      # Reset prefilecount before repack call
      prefilecount=`ls -1 $hfiles | wc -l`

      # Execute the repack function as long as prefilecount != 0
      if [ $prefilecount -ne 0 ]; then
         repack
      fi

      # Regrab file count. If this number is different than prefilecount, the 
      # repack function will execute again
      postfilecount=`ls -1 $hfiles | wc -l`         
      echo "prefilecount, postfilecount: $prefilecount $postfilecount"

      # execute time controller
      sleep $checkint

      # Check again for job name to see if it finished
      # Determine job string for repack execution
      if [ $exectype -eq 1 ]; then       # Standard
         jobout=$(pidof -s $jobname)
      elif [ $exectype -eq 2 ]; then     # LSF
         jobout=$(bjobs -l -J $jobname | grep -c $jobname)
         if [ $jobout -eq 0 ]; then jobout=""; fi
      elif [ $exectype -eq 3 ]; then     # Grid Engine
         #jobout=$(qstat -j $jobname | grep -c $jobname)
         jobout=$(qstat -f | grep -c $jobname)
         if [ $jobout -eq 0 ]; then jobout=""; fi
      fi
      echo "pid or jobname count: $jobout"
   done

   # One final check after while loop to catch the ending file
   repack

   # We are done
   echo $jobname" finished and x.repack.sh is terminating"

else # repack files for a run that is already done

   echo "Repacking files in directory $thisdir"
   repack
   echo "x.repack.sh finished for directory $thisdir"
   exit

fi

exit

###############################################################


