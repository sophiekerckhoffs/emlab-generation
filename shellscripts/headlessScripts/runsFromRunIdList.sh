USAGE="Provide name of run and number of runs"
#Load configuration script to substitute
if [ -f scriptConfigurations.cfg ];then 
	. scriptConfigurations.cfg
	HOME=$REMOTERESULTFOLDER
else
    echo "Define scriptConfigurations.cfg, by changing the template. Exiting script."
    exit
fi

#Alternative way to define non sequential scenario runs.
#RUNS=(14 21 26 32 38  72 84 108 111 120 132 156 160 162 164 168 180)

FILE=$1
NUMBERROFRUNSPERSCENARIO=1
PAUSE="2s"
WALLTIME=08:00:00

numberOfRunIds=$(cat $FILE | wc -l)
echo "Running "$numberOfRunIds" runIds."
no=0

#for i in "${RUNS[@]}"
for runId in $(cat $FILE)
do
#    SCENARIO=$SCENARIONAME"-$i"
 #   $REMOTEHPCSCRIPTS/hpcArrayRun.sh $RUNNAME $RUNNAME"-$i" $SCENARIO".xml" $NUMBERROFRUNSPERSCENARIO $WALLTIME
#$RUNNAME $RUNNAME"-$i" $SCENARIO".xml" $NUMBERROFRUNSPERSCENARIO $WALLTIME 
#echo $runId
runId=$(echo $runId | sed 's/.xml$//')
#echo $runId
RUNNAME=$(echo $runId | sed 's/-[0-9]*$//')
#SCENARIONAME=$(echo $runId | sed 's/^MWC/MWB/')".xml"
SCENARIONAME=$runId".xml"
#echo $RUNNAME
#echo $SCENARIONAME
echo  "$REMOTEHPCSCRIPTS/hpcArrayRun.sh $RUNNAME $runId $SCENARIONAME $NUMBERROFRUNSPERSCENARIO $WALLTIME"
$REMOTEHPCSCRIPTS/hpcArrayRun.sh $RUNNAME $runId $SCENARIONAME $NUMBERROFRUNSPERSCENARIO $WALLTIME

sleep $PAUSE
$((no++))
echo $no

if [ $no -eq 120 ] 
then
    no=0
    echo "Taking 2h break"
    sleep 1m
fi
done

./mailScript.sh