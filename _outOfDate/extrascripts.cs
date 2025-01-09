using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using System.IO;

using UnityEngine.InputSystem;
using UnityEngine.XR.MagicLeap;
using UnityEngine.UI;


public class TaskManager : MonoBehaviour
{
    public enum socialModes {
        experimenterMode,
        ready4Connection,
        connectedIdle,
        repositioning,
        collecting,
        pindropping,
        watchingCollection,
        watchingPinDropping,
        voting,
        collectStandBy,
        pindropStandBy,
        watchCollectingStandBy,
        watchPinDroppingStandBy,
        pindropVoting,
        pindropWalk2Coin
    }
    socialModes mode;
    socialModes mode2start;

    public bool mainPart;
    public Text uitext;
    public Text labelui;
    public Text rewardUI;
    public Text rewardTempUI;
    public Text totalRewardUI;

    public List<Transform> pathPoints;
    public LineRenderer pathLine;
    public Material selectedMatPathPoint;
    public Material normalMatPathPoint;
    public GameObject collectableCoinPrefab;
    Transform pathTransform;
    public bool pathEditing;
    int pathPointsel;
    public int coinsetID;
    int coinAmount;

    public Transform headTransform;
    [Tooltip("In hertz HZ, using 50 for 50Hz, would log every 20 miliseconds")]
    public float lograte = 50f;
    public string filenameCore = "ObsReward";
    float logtimer;
    FileStream logstream;
    StreamWriter logwriter;
    System.DateTime timeZero;
    System.DateTime clocker;
    string logname;
    string timestamp;
    string loginfo;
    bool logopen;
    
    RaycastHit hit;
    bool good2add;
    public Vector3 addpointpos;
    PersistentAnchor persistentAnchor;    

    MagicLeapInputs magicLeapInputs;
    MagicLeapInputs.ControllerActions controllerActions;

    
    Transform controlTransform;
    GameObject anchorIndication;

    Transform pathPointTransform;
    public Transform newPathPointIcon;
    float lastTriggerTime = -1f;
    float quitTimerStart = -1f;
    
    Transform controlPointer;
    Vector3 pointerpos;
    Vector3 pointersize;
    public float pointeroffset = 0.25f;
    bool pathChanged;

    public Transform trailObject;
    //object just to register the collisions for the eyetracking ray
    Transform trailFakeBody;
    Vector3 trailFakeBodyPos;
    Vector3 trailFakeBodyScale;
    public float trailPosGain = 10f;
    int taskBlock_i;

    public GameObject VotingPanel;
    public Color votingColor = new Color(220, 224, 120); 
    public Color neutralVotingColor = new Color(90, 92, 128);
    Image voteButton;
    Vector3 newtrailpos;
    float trailFXtimer;
    public float trailFXRate = 2;    

    //to keep track of current coin
    int nextcoin_i = 0;
    float reward = 0f;
    float validatedReward = 0f;
    float totalReward = 0f; //acumulated across all blocks
    AudioSource maudio;
    public AudioClip coinCollectionSound;
    public AudioClip rewardSound;
    public AudioClip penaltySound;
    public AudioClip pindropSound;
    public GameObject pinGO;
    public string getReady2CollectMsg = "Press trigger to start collecting";
    public string getReady2PindropMsg = "Press trigger to start pindropping";
    public string startCollectingMsg = "Walk to a visible coin to collect it";
    public string finishCollectionMsg = "Finished collecting. Please standby";
    public string startPinDropMsg = "Pull the trigger when ready to start PinDropping";
    public string pindropMsg = "Pull trigger to drop a pin where your hand is";
    public string pindropMsg2 = "Please repeat the pindrop round";
    public string endOfroundMsg = "Pull trigger again to finish this round";
    public string finishDroppingMsg = "Finished all pins, please standby";
    public string finishWatchCollectionMsg = "Finished watching. Please standby";
    public string finishWatchPinDroppingMsg = "Finished watching. Please standby";
    public string watchCollectGetReadyMsg = "Get ready to watch a collection round";
    public string watchPindropGetReadyMsg = "Get ready to watch a pin dropping round";
    public string watchCollectMsg = "Watch a collection round";
    public string watchPindropMsg = "Watch a pin dropping round and vote using the bumper for a valid(existing) location, otherwise the trigger";
    public string notyetfinishedWatching = "not yet finished watching round, please wait";
    public string pindropVoteMsg = "bumper=valid trigger=invalid";
    public string pindropResMsg = "bumper=swaped round trigger=not swaped";
    public string positionMsg = "please move to the indicated position";
    public string positionReadyMsg = "please standby";
    public float uiDisplayTime = 5.0f;
    public float rewardDisplayTime = 3.0f;
    bool decidingSurprise = false;
    bool otherSwapvoted = false;
    bool lastdropgood;
    public bool showReward2Observer;

    string msgcommstr;
    bool newcommmsg;
    public Queue<ObsRewardComm.TerminalCmds> terminalCmd;
    //ulong otherUIDfirst, otherUIDsec;
    //float otherPartAnchorx, otherPartAnchory, otherPartAnchorz;
    //float otherPartAnchorfx, otherPartAnchorfy, otherPartAnchorfz, otherPartAnchorfw;

    public float openChestSpeed=2f;
    public float delayedRevealTime=1f;
    CoinValue activeChest;

    List<int> pathids;
    List<float> ppointsx, ppointsy, ppointsz;
    List<float> coinValues;

    float 
        otherpartx, 
        otherparty, 
        otherpartz;
    float
        otherpartRawx,
        otherpartRawy,
        otherpartRawz;
    float 
        otherpindropx, 
        otherpindropy, 
        otherpindropz;
    float
        otherpindropRawx,
        otherpindropRawy,
        otherpindropRawz;

    List<GameObject> droppedPins;

    public float time4pinfeedback = 2.0f;
    public Material goodPinMat;
    public Material badPinMat;
    public float scoringDist = 1.6f;
    bool perfectDroprun = false;
    int dropround_i=0;
    int perfectruns = 0;
    int dropround = 0;
    public int perfectRoundsTarget;
    public float feedbackCoinDelay = 0.5f;
    public GameObject fdbkGoodcoin;
    public GameObject fdbkBadcoin;
    public GameObject fdbkSpecialcoin;
    GameObject observedFeedbackcoin;
    bool observedCoinAlreadyCollected;
    Renderer observedPinRender;
    Vector3 obscoinpos;
    float observedCoinValue;
    public float specialThreshold=1.0f;
    public Image timer4pindropVote_ui;
    float timer4pindropVote;
    int closestChest_i;
    float closestdist;
    float timer4pindropVoteFlash;
    bool timer4pindropVoteFlashing;

    Vector3 position2startBlock;
    public float pos2startblockx, pos2startblocky, pos2startblockz;
    bool ready2startBlock;
    public bool otherReady2startBlock;
    public Transform initialposIndicator;
    public string resetpositions;

    Vector3 headPosABase;
    Vector3 headForthABase;
    Vector3 gazeABase;
    Vector3 fixationPointABase;

    public Transform optiBodyA;
    public Transform optiBodyB;
    public MotiveExample motiveExample;

    int rec_dropround = 0;
    int rec_perfectruns = 0;
    float rec_reward = 0f;    

    // Start is called before the first frame update
    void Start()
    {
        maudio = GetComponent<AudioSource>();
        pathids = new List<int>();
        ppointsx = new List<float>();
        ppointsy = new List<float>();
        ppointsz = new List<float>();
        coinValues = new List<float>();
        
        droppedPins = new List<GameObject>();

        pathTransform = pathPoints[0].parent;
        controlPointer = transform.GetChild(0);
        pointerpos = new Vector3(0f,0f,1f);
        pointersize = new Vector3(0.0015f,1f,0.0015f);
                
        hit = new RaycastHit();

        magicLeapInputs = new MagicLeapInputs();
        magicLeapInputs.Enable();
        controllerActions = new MagicLeapInputs.ControllerActions(magicLeapInputs);
        controllerActions.Bumper.performed += HandleOnBumper;
        controllerActions.TriggerButton.performed += HandleOnTrigger;
        controllerActions.Trigger.canceled += HandleTriggerUp;

        MLDevice.RegisterGestureSubsystem();                   

        mode = socialModes.experimenterMode;
        uitext.text = "Bumper to define anchor or double Trigger to connect to terminal";
        reward = 0;
        validatedReward = 0f;
        totalReward = 0f;
        rewardTempUI.text = "";
        rewardUI.text = "version "+(mainPart?"A":"B")+ObsRewardComm.HeadsetComm.versionNum;
        totalRewardUI.text = "";
        lastTriggerTime = -1f;

        terminalCmd = new Queue<ObsRewardComm.TerminalCmds>();
        persistentAnchor = GetComponent<PersistentAnchor>();
        anchorIndication = persistentAnchor.persistentTarget.GetChild(0).gameObject;
        controlTransform = transform;
        //anchorIndication.SetActive(false);

        //initializing fake body variables
        trailFakeBody = trailObject.GetChild(0);
        trailFakeBodyPos = trailFakeBody.localPosition;
        trailFakeBodyScale = trailFakeBody.localScale;
        //test
        trailObject.position = new Vector3(1.6f,1.6f,3f);
        AdjustFakeTrailBody(trailObject.position.y);
        trailObject.gameObject.SetActive(false);

        ShowCoins(false);
    }

    // Update is called once per frame
    void Update()
    {
        #if UNITY_EDITOR
        if (Input.GetKeyDown(KeyCode.O))
        {
            string hostName = System.Net.Dns.GetHostName();
            var host = System.Net.Dns.GetHostEntry(hostName);
            foreach (var ip in host.AddressList)
            {
                if (ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                {
                    Debug.Log("IP try: " + ip.ToString());
                    motiveExample.localIPAddress = ip.ToString();
                }
            }       
            //motiveExample.localIPAddress = "192.168.50.138";
            //enable and start optitrack feed
            optiBodyA.gameObject.SetActive(true);
            optiBodyB.gameObject.SetActive(true);
            optiBodyA.GetComponent<Renderer>().enabled = true;
            optiBodyB.GetComponent<Renderer>().enabled = true;
            motiveExample.ConnectFun();
        }
        #endif

        //logging
        if(Logger.fileopen) { KeepLogging(); }

        switch (mode) {
            case socialModes.experimenterMode:
                #if UNITY_EDITOR
                    if (Input.GetKeyDown(KeyCode.Return))
                    {
                        SetReady();
                        mode = socialModes.connectedIdle;
                        Debug.Log("ready to start the tests in unity");
                        uitext.text = "idle: C-for collecting, W-see other's trail";

                        /*
                        //test new chest transform
                        GameObject newpoint = GameObject.Instantiate(collectableCoinPrefab, pathTransform);
                        CoinValue thiscoin = newpoint.GetComponent<CoinValue>();
                        thiscoin.coinValue = 124f;
                        thiscoin.SetupAudio(maudio, rewardSound,penaltySound);
                        thiscoin.headTransform = trailObject;
                        FeedbackCoin thisfcoin = newpoint.GetComponent<FeedbackCoin>();
                        if (thisfcoin)
                            Debug.Log("weird, chest doesnt have the feedback script");
                        else
                            Debug.Log("good, chest indeed does not have feedbackcoin script");
                        */
                        //observedFeedbackcoin = Instantiate(fdbkSpecialcoin);
                        //observedFeedbackcoin.transform.position = Vector3.zero + 1.2f*Vector3.up;
                    }
                    if (Input.GetKeyDown(KeyCode.O)) {
                        trailObject.position = new Vector3(1.6f, 1.6f, 3f);
                        AdjustFakeTrailBody(trailObject.position.y);
                        trailObject.gameObject.SetActive(true);
                    }
                #endif
                //testing highlights to remove path points
                if (pathEditing) {
                    #if UNITY_EDITOR
                    if (Input.GetKeyDown(KeyCode.RightArrow))
                    {
                        if (pathPointsel >= 0)
                            UnselectPathPoint(pathPoints[pathPointsel]);
                        pathPointsel++;
                        if (pathPointsel >= pathPoints.Count)
                            pathPointsel = 0;
                        SelectPathPoint(pathPoints[pathPointsel]);
                    }
                    else if (Input.GetKeyDown(KeyCode.LeftArrow))
                    {
                        UnselectPathPoint(pathPoints[pathPointsel]);
                        pathPointsel--;
                        if (pathPointsel < 0)
                            pathPointsel = pathPoints.Count - 1;
                        SelectPathPoint(pathPoints[pathPointsel]);
                    }
                    else if (Input.GetKeyDown(KeyCode.UpArrow)) {
                        RemovePointAt(pathPoints[pathPointsel]);
                    }
                    else if (Input.GetKeyDown(KeyCode.DownArrow))
                    {
                        AddCoinPoint(new Vector3(Random.Range(-5f, 5f), 1f, Random.Range(-5f, 5f)),2);
                    }
                    #endif
                    //raycast from controller to check if user is pointing to a pathPoint                
                    RaycastHit hitInfo;
                    if (Physics.Raycast(controlTransform.position, controlTransform.forward, out hitInfo))
                    {
                        if (hitInfo.transform.tag == "Finish")
                        {
                            if (!pathPointTransform)
                            {
                                SelectPathPoint(hitInfo.transform);
                            }
                            //highlight pathpoint
                            pathPointTransform = hitInfo.transform;
                            controlPointer.gameObject.SetActive(true);
                            pointerpos.z = (hitInfo.distance / 2) + pointeroffset/2;
                            pointersize.y = (hitInfo.distance - pointeroffset) / 2;
                            controlPointer.localPosition = pointerpos;
                            controlPointer.localScale = pointersize;
                            newPathPointIcon.gameObject.SetActive(false);
                            good2add = false;

                            #if UNITY_EDITOR
                            if (Input.GetKeyDown(KeyCode.Space))
                                EditPath(pathPointTransform);
                            #endif
                        }
                        else if (hitInfo.transform.tag == "Respawn")
                        {
                            if (pathPointTransform)
                                UnselectPathPoint(pathPointTransform);
                            pathPointTransform = null;
                            //show new pathpoint item on the ground
                            newPathPointIcon.gameObject.SetActive(true);
                            newPathPointIcon.position = hitInfo.point;
                            controlPointer.gameObject.SetActive(true);
                            pointerpos.z = (hitInfo.distance / 2) + pointeroffset/2;
                            pointersize.y = (hitInfo.distance - pointeroffset) / 2;
                            controlPointer.localPosition = pointerpos;
                            controlPointer.localScale = pointersize;
                            good2add = true;
                            addpointpos = hitInfo.point;
                            #if UNITY_EDITOR
                            if (Input.GetKeyDown(KeyCode.Space))
                                EditPath(pathPointTransform);
                            #endif
                        }
                        else {
                            good2add = false;
                            if (pathPointTransform)
                                UnselectPathPoint(pathPointTransform);
                            pathPointTransform = null;
                            controlPointer.gameObject.SetActive(false);
                        }
                    }
                    else
                    {
                        if (pathPointTransform)
                            UnselectPathPoint(pathPointTransform);
                        pathPointTransform = null;
                        newPathPointIcon.gameObject.SetActive(false);
                        controlPointer.gameObject.SetActive(false);
                        good2add = false;
                    }
                }
                else{
                    newPathPointIcon.gameObject.SetActive(false);
                    controlPointer.gameObject.SetActive(false);
                }
                break;
            case socialModes.connectedIdle:
                #if UNITY_EDITOR
                    if (Input.GetKeyDown(KeyCode.C))
                        StartCollecting();
                    if (Input.GetKeyDown(KeyCode.W)){
                        mode = socialModes.watchingCollection;
                        StartObservedTrail(pathPoints[0].position);
                    }
                    if(Input.GetKeyDown(KeyCode.S))
                        StartVoting();
                #endif
                break;
            case socialModes.collecting:
                //update terminal position
                trailFXtimer -= Time.deltaTime;
                if (trailFXtimer < 0f) {
                    trailFXtimer = (float)(1.0f/trailFXRate);
                    UpdateCurrPosNSend();
                }
                break;
            case socialModes.pindropping:
                //update terminal position
                trailFXtimer -= Time.deltaTime;
                if (trailFXtimer < 0f)
                {
                    trailFXtimer = (float)(1.0f / trailFXRate);
                    UpdateCurrPosNSend();
                }
                break;
            case socialModes.pindropWalk2Coin:
                //update terminal position
                trailFXtimer -= Time.deltaTime;
                if (trailFXtimer < 0f)
                {
                    trailFXtimer = (float)(1.0f / trailFXRate);
                    UpdateCurrPosNSend();
                }
                break;
            case socialModes.voting:
                RaycastHit hitInfov;
                if (Physics.Raycast(controlTransform.position, controlTransform.forward, out hitInfov))
                {
                    controlPointer.gameObject.SetActive(true);
                    pointerpos.z = (hitInfov.distance / 2) + pointeroffset / 2;
                    pointersize.y = (hitInfov.distance - pointeroffset) / 2;
                    controlPointer.localPosition = pointerpos;
                    controlPointer.localScale = pointersize;

                    //Debug.Log("voting ray: "+hitInfov.collider.name);
                    if (hitInfov.transform.tag == "Finish")
                    {
                        if (voteButton == null) {
                            voteButton = hitInfov.transform.GetComponent<Image>();
                            voteButton.color = votingColor;
                        }
                    }
                    else {
                        if (voteButton != null)
                        {
                            voteButton.color = neutralVotingColor;
                            voteButton =null;
                        }
                    }
                }
                else {
                    //Debug.Log("voting ray not hitting anything");
                    controlPointer.gameObject.SetActive(false);
                    if (voteButton != null)
                    {
                        voteButton.color = neutralVotingColor;
                        voteButton = null;
                    }
                }
                #if UNITY_EDITOR
                    //fake vote
                    if(Input.GetKeyDown(KeyCode.V)){
                        if (voteButton)
                        {
                            Vote(voteButton.name.Replace("Vote", ""));
                        }
                    }
                #endif
                break;
            case socialModes.watchingCollection:
                //update trail according to the other participant's position
                trailObject.position = Vector3.Lerp(trailObject.position, newtrailpos, trailPosGain*Time.deltaTime);
                AdjustFakeTrailBody(trailObject.position.y);
                #if UNITY_EDITOR
                    //fake other participant's position with a perfect path for debug
                    if(Vector3.Distance(trailObject.position, pathPoints[nextcoin_i].position) < 0.5f && nextcoin_i < coinAmount-1){
                        nextcoin_i++;
                        newtrailpos = pathPoints[nextcoin_i].position;
                    }
                    if(Input.GetKeyDown(KeyCode.V))
                        StartVoting();
                #endif
                break;
            case socialModes.watchingPinDropping:
                //update trail according to the other participant's position
                trailObject.position = Vector3.Lerp(trailObject.position, newtrailpos, trailPosGain*Time.deltaTime);
                AdjustFakeTrailBody(trailObject.position.y);
                #if UNITY_EDITOR
                    //fake other participant's position with a perfect path for debug
                    if(Vector3.Distance(trailObject.position, pathPoints[nextcoin_i].position) < 0.5f && nextcoin_i < coinAmount-1){
                        nextcoin_i++;
                        newtrailpos = pathPoints[nextcoin_i].position;
                    }
                    if(Input.GetKeyDown(KeyCode.V))
                        StartVoting();
                #endif
                break;
            case socialModes.pindropVoting:
                timer4pindropVote_ui.fillAmount = timer4pindropVote / time4pinfeedback;
                timer4pindropVote -= Time.deltaTime;

                //make the ui flash
                timer4pindropVoteFlash -= Time.deltaTime;
                if(timer4pindropVoteFlash<0f) {
                    if (timer4pindropVoteFlashing)
                    {
                        timer4pindropVoteFlash = 0.25f;
                        uitext.text = "";
                    }
                    else
                    {
                        timer4pindropVoteFlash = 0.5f;
                        uitext.text = pindropVoteMsg;
                    }
                    timer4pindropVoteFlashing = !timer4pindropVoteFlashing;
                    
                }
                if (timer4pindropVote <= 0f)
                    DroppedPinVote(0);
                break;
            case socialModes.repositioning:
                trailObject.gameObject.SetActive(false);
                initialposIndicator.localPosition = position2startBlock;
                if (!ready2startBlock)
                {
                    float dist2repos = Vector3.ProjectOnPlane(headTransform.position - initialposIndicator.position, Vector3.up).magnitude;
                    if (dist2repos < scoringDist)
                    {
                        Logger.Loginfo(DataType: "Event", LogMessage: "Repositioned and ready to start block or round", showmsg: true);
                        //send message to signal ready2startBlock
                        ObsRewardComm.HeadsetComm.UpdateStatus(ObsRewardComm.TerminalCmds.T_Repositioned2startBlock, "repositioned and ready to start block or round");
                        ready2startBlock = true;
                        uitext.text = positionReadyMsg;
                    }
                    //else {
                    //    uitext.text = "reposition distance: " + dist2repos;
                    //}
                }
                if (ready2startBlock && otherReady2startBlock) {
                    BlockStart();
                }
                break;
        }        

        if (newcommmsg)
        {
            newcommmsg = false;
            uitext.text = msgcommstr;
            //StartCoroutine(ClearUI());
        }

        //commands from the terminal
        if (terminalCmd.Count > 0) {
            CheckTerminalCommand();
        }
    }

    void CheckTerminalCommand() {
        ObsRewardComm.TerminalCmds aterminalCmd = terminalCmd.Dequeue();
        switch (aterminalCmd)
        {
            case ObsRewardComm.TerminalCmds.T_GeoInfo:
                Debug.Log("Received geodata from terminal");
                /*
                Vector3 otherPartAnchorpos = new Vector3(otherPartAnchorx, otherPartAnchory, otherPartAnchorz);
                Quaternion otherPartAnchorforth = new Quaternion(otherPartAnchorfx, otherPartAnchorfy, otherPartAnchorfz, otherPartAnchorfw);
                UnityEngine.XR.MagicLeap.Native.MagicLeapNativeBindings.MLCoordinateFrameUID CFUID;
                CFUID = new UnityEngine.XR.MagicLeap.Native.MagicLeapNativeBindings.MLCoordinateFrameUID();
                CFUID.First = otherUIDfirst;
                CFUID.Second = otherUIDsec;                
                Debug.Log("original PCF ID first:" + persistentAnchor.anchorBinding.PCF.CFUID.First);
                Debug.Log("original PCF ID sec:" + persistentAnchor.anchorBinding.PCF.CFUID.Second);
                Debug.Log("Got all geodata from terminal, trying to manually bind");
                persistentAnchor.ManualPCFBind(CFUID, otherPartAnchorpos, otherPartAnchorforth);
                Debug.Log("Should be updated now");
                */

                //update the path points too
                //clear old points
                for (int i = 0; i < pathPoints.Count; i++)
                {
                    Destroy(pathPoints[i].gameObject);
                }
                pathPoints.Clear();
                Debug.Log("Removed old coinpoints");
                //add the new ones
                InitCoinPoints();
                Debug.Log("Added new coinpoints from terminal");
                ShowCoins(false);
                Debug.Log("all set up and ready to start task");
                break;
            case ObsRewardComm.TerminalCmds.T_StopExperiment:
                uitext.text = "task ended by experimenter";

                CutTaskShort();

                CloseLog();
                break;
            case ObsRewardComm.TerminalCmds.T_TaskComplete:
                uitext.text = "Task is complete, thank you.";
                CloseLog();
                break;
            case ObsRewardComm.TerminalCmds.T_PauseTask:
                Time.timeScale = 0f;
                Logger.Loginfo(DataType: "Event", LogMessage: "Task was paused", showmsg: true);
                //send flag to terminal to confirm task is paused
                ObsRewardComm.HeadsetComm.UpdateStatus(ObsRewardComm.TerminalCmds.T_PauseTask, "task paused");
                break;
            case ObsRewardComm.TerminalCmds.T_UnPauseTask:
                Time.timeScale = 1f;
                Logger.Loginfo(DataType: "Event", LogMessage: "Task was resumed", showmsg: true);
                //send flag to terminal to confirm task resumed
                ObsRewardComm.HeadsetComm.UpdateStatus(ObsRewardComm.TerminalCmds.T_UnPauseTask, "task resumed");
                break;
            case ObsRewardComm.TerminalCmds.T_StartCollecting:
                //mode to start after repositioning
                mode2start = socialModes.collectStandBy;
                EnterRepositioning();
                StartLog();
                Logger.Loginfo(DataType: "Event", LogMessage: "Mark should happen if checked on terminal.");
                break;
            case ObsRewardComm.TerminalCmds.T_StartPinDropping:
                InitCoinPoints();

                dropround = 0;
                dropround_i = 0;
                perfectruns = 0;
                reward = 0f;
                validatedReward = 0f;

                mode2start = socialModes.pindropStandBy;
                EnterRepositioning();

                StartLog();
                Logger.Loginfo(DataType: "Event", LogMessage: "Mark should happen if checked on terminal.");
                LogCoinPoints();
                ShowCoins(false);
                break;
            case ObsRewardComm.TerminalCmds.T_StartPinDroppingWRec:
                InitCoinPoints();

                //trying to recover
                dropround = rec_dropround;
                perfectruns = rec_perfectruns;
                reward = 0f;
                validatedReward = rec_reward;                
                
                dropround_i = 0;
                mode2start = socialModes.pindropStandBy;
                EnterRepositioning();

                StartLog();
                Logger.Loginfo(DataType: "Event", LogMessage: "Recovering previous pin dropping. Block:" + taskBlock_i);
                Logger.Loginfo(DataType: "Event", LogMessage: "Mark should happen if checked on terminal.");
                LogCoinPoints();
                ShowCoins(false);
                break;
            case ObsRewardComm.TerminalCmds.T_GetReady2watchCollecting:
                mode2start = socialModes.watchCollectingStandBy;
                EnterRepositioning();
                StartLog();
                Logger.Loginfo(DataType: "Event", LogMessage: "Mark should happen if checked on terminal.");
                break;
            case ObsRewardComm.TerminalCmds.T_StartWatchCollecting:
                if (mode != socialModes.connectedIdle)
                    CutTaskShort();
                StartObservedTrail(new Vector3(otherpartx, otherparty, otherpartz));
                Logger.globalBlock++;
                Logger.Loginfo(DataType: "Event", LogMessage: "Started watching other participant's collecting. Block:"+taskBlock_i);
                mode = socialModes.watchingCollection;
                nextcoin_i = 0;
                InitCoinPoints();
                LogCoinPoints();
                pathPoints[nextcoin_i].gameObject.SetActive(true);
                activeChest = pathPoints[nextcoin_i].gameObject.GetComponent<CoinValue>();
                uitext.text = watchCollectMsg;
                StartCoroutine(ClearUI());
                break;
            case ObsRewardComm.TerminalCmds.T_GetReady2watchPinDropping:
                Logger.globalBlock++;
                mode2start = socialModes.watchPinDroppingStandBy;
                EnterRepositioning();

                StartLog();
                Logger.Loginfo(DataType: "Event", LogMessage: "Mark should happen if checked on terminal.");

                InitCoinPoints();
                LogCoinPoints();
                ShowCoins(false);

                perfectruns = 0;
                dropround = 0;
                dropround_i = 0;
                reward = 0f;
                validatedReward = 0f;

                nextcoin_i = 0;
                perfectDroprun = true;
                break;
            case ObsRewardComm.TerminalCmds.T_GetReady2watchPinDroppingWRec:
                Logger.globalBlock++;
                mode2start = socialModes.watchPinDroppingStandBy;
                EnterRepositioning();

                StartLog();
                Logger.Loginfo(DataType: "Event", LogMessage: "Mark should happen if checked on terminal.");
                InitCoinPoints();
                LogCoinPoints();
                ShowCoins(false);
                
                dropround = rec_dropround;
                perfectruns = rec_perfectruns;
                reward = 0f;
                validatedReward = rec_reward;
                
                perfectDroprun = true;
                Logger.Loginfo(DataType: "Event", LogMessage: "Recovering previous watch pin dropping.");
                
                nextcoin_i = 0;
                dropround_i = 0;
                break;
            case ObsRewardComm.TerminalCmds.T_StartWatchPindropping:
                if (mode != socialModes.connectedIdle)
                    CutTaskShort();
                StartObservedTrail(new Vector3(otherpartx, otherparty, otherpartz));
                Logger.Loginfo(DataType: "Event", LogMessage: "Started watching other participant's pin dropping. Block: "+taskBlock_i);
                mode = socialModes.watchingPinDropping;
                nextcoin_i = 0;                
                uitext.text = watchPindropMsg;
                droppedPins.Clear();                
                break;
            case ObsRewardComm.TerminalCmds.T_StartVoting:
                if (mode != socialModes.connectedIdle)
                    CutTaskShort();
                StartVoting();
                break;
            case ObsRewardComm.TerminalCmds.T_BecomeMain:
                SetMainParticipant(true);
                Debug.Log("Received flag to become Main");
                break;
            case ObsRewardComm.TerminalCmds.T_BecomeSec:
                SetMainParticipant(false);
                Debug.Log("Received flag to become Secondary participant");
                break;
            case ObsRewardComm.TerminalCmds.T_NewWatchedPos:
                UpdateOtherParticipantPos();
                newtrailpos.x = otherpartx;
                newtrailpos.y = otherparty;
                newtrailpos.z = otherpartz;
                //uitext.text = "otherpos up";
                //StartCoroutine(ClearUI(0.3f));
                break;
            case ObsRewardComm.TerminalCmds.T_PinDrop:
                maudio.PlayOneShot(pindropSound);
                //instantiate the new pin
                UpdateOtherPinDrop();
                Vector3 newpinpos = new Vector3(otherpindropx, otherpindropy, otherpindropz);
                GameObject droppedPin = GameObject.Instantiate(pinGO, newpinpos, Quaternion.identity);
                observedPinRender = droppedPin.transform.GetChild(0).GetComponent<Renderer>();
                droppedPins.Add(droppedPin);

                //Log other participant dropped pin
                Logger.Loginfo(DataType: "Event", LogMessage: "Other participant just dropped a new pin at " + 
                    newpinpos.x.ToString("0.000")+" "+
                    newpinpos.y.ToString("0.000")+" "+
                    newpinpos.z.ToString("0.000"));

                uitext.text = pindropVoteMsg;
                timer4pindropVoteFlashing = true;
                timer4pindropVoteFlash = 1.0f;
                //StartCoroutine(ClearUI());

                //start voting timer
                timer4pindropVote = time4pinfeedback;
                //show the voting timer indicator
                timer4pindropVote_ui.enabled = true;
                timer4pindropVote_ui.fillAmount = 1.0f;

                //change mode for pinvoting
                mode = socialModes.pindropVoting;
                
                //calculate what chest location is closer to show the feedback coin
                closestdist = -1f;
                closestChest_i = 0;
                for (int i = 0; i < pathPoints.Count; i++)
                {
                    float thisdist = (pathPoints[i].position - newpinpos).magnitude;
                    if (closestdist < 0f || thisdist < closestdist)
                    {
                        closestdist = thisdist;
                        closestChest_i = i;
                    }
                }

                observedCoinValue = pathPoints[closestChest_i].GetComponent<CoinValue>().coinValue;
                obscoinpos = pathPoints[closestChest_i].position + 1.2f * Vector3.up;

                //log whether or not the pin was dropped on a valid location and which one it was
                string droppedPinLocationMsg = "Dropped pin was dropped at "+closestdist.ToString("0.00")+" from chest "+closestChest_i+" originally at "+pathPoints[closestChest_i].position;
                if (closestdist<scoringDist) {
                    droppedPinLocationMsg += ":CORRECT";
                    lastdropgood = true;
                }
                else { 
                    droppedPinLocationMsg += ":INCORRECT";
                    lastdropgood = false;
                }
                Logger.Loginfo(DataType: "Event", LogMessage: droppedPinLocationMsg, showmsg: true);

                //remove coin point from pathpoints
                Destroy(pathPoints[closestChest_i]);
                pathPoints.RemoveAt(closestChest_i);

                //to allow the feedback coin to appear
                observedCoinAlreadyCollected = false;

                break;
            case ObsRewardComm.TerminalCmds.T_FeedbackCoinCollected:
                //hide previous pin
                if (droppedPins.Count > 0)
                {
                    droppedPins[droppedPins.Count - 1].gameObject.SetActive(false);
                }                                
                
                if (observedFeedbackcoin)
                {
                    if(observedCoinValue < 0.00001f)
                    {
                        maudio.PlayOneShot(penaltySound);
                    }
                    else if(observedCoinValue < specialThreshold)
                    {
                        maudio.PlayOneShot(penaltySound);
                    }
                    else { maudio.PlayOneShot(rewardSound); }
                    Destroy(observedFeedbackcoin);
                }
                else {
                    observedCoinAlreadyCollected = true;
                }

                float temprew = 0;
                if (lastdropgood && perfectDroprun)
                {
                    //to incentivate the player to go first on the big fish, the first 2 coins always give doubled the reward
                    if (nextcoin_i < 2)
                    {
                        temprew = 2 * observedCoinValue;
                    }
                    else
                    {
                        temprew = observedCoinValue;
                    }
                    reward += temprew;
                }
                else { 
                    reward = 0f;
                    validatedReward = 0f;
                    perfectDroprun = false;
                    perfectruns = 0;
                }

                if (showReward2Observer)
                {
                    rewardTempUI.text = "$" + temprew.ToString("0.00");
                    StartCoroutine(ClearRewardTemp());
                    //rewardUI.text = "AN: $" + reward.ToString("0.00");
                }

                //count and restart coins
                nextcoin_i++;
                if (nextcoin_i >= coinAmount)
                {
                    if (lastdropgood && perfectDroprun)
                    {
                        perfectruns++;
                        validatedReward += reward;
                        if (perfectruns >= perfectRoundsTarget)
                        {
                            totalReward += validatedReward;
                        }
                        Logger.Loginfo(DataType: "Event", LogMessage: "A.N. finished a perfect dropround with:" + reward.ToString("0.00") + " total reward: " + totalReward.ToString("0.00"));
                    }
                    else{
                        perfectruns = 0;
                        validatedReward = 0f;
                        reward = 0f;
                    }
                    totalRewardUI.text = "";
                    nextcoin_i = 0;
                    perfectDroprun = true;
                    StartCoroutine(DelayedDroppedPinsDisappear());
                }

                break;
            case ObsRewardComm.TerminalCmds.T_LearnedPins:
                if(decidingSurprise){
                    otherSwapvoted = true;
                }
                else
                {
                    FinishPindropWatching();
                    rewardUI.text = "";
                    totalRewardUI.text = "";
                }
                break;
            case ObsRewardComm.TerminalCmds.T_ShowLabels:
                if (mainPart)
                    labelui.text = "A";
                else
                    labelui.text = "B";
                StartCoroutine(HideLabels());
                break;
            case ObsRewardComm.TerminalCmds.T_ShowCoins:
                ShowCoins(true);
                break;
            case ObsRewardComm.TerminalCmds.T_HideCoins:
                ShowCoins(false);
                break;
            /*
        case ObsRewardComm.TerminalCmds.T_newCoinsetId:
            //update path according to new coinsetID
            InitCoinPoints();
            LogCoinPoints();
            ShowCoins(true);
            break;
            */
            case ObsRewardComm.TerminalCmds.T_CollectedCoin:
                //show next coin
                int collected_i = nextcoin_i;
                CoinValue thiscoin = pathPoints[nextcoin_i].GetComponent<CoinValue>();
                if (thiscoin)
                {
                    reward += thiscoin.coinValue;
                    rewardUI.text = "AN: $" + reward.ToString("0.00");
                }
                //Log other participant just collected a coin
                Logger.Loginfo(DataType: "Event", LogMessage: "Other participant just collected coin: " + collected_i);
                maudio.PlayOneShot(coinCollectionSound);
                break;
            case ObsRewardComm.TerminalCmds.T_ChestOpened:
                if(activeChest)
                    activeChest.OpenChest(true, this);
                break;
            case ObsRewardComm.TerminalCmds.T_RequestReposition:
                //only the observer receives the request to reposition
                mode2start = socialModes.watchingPinDropping;

                if (resetpositions.Contains('|'))
                {
                    if (resetpositions.Split('|').Length >= 3)
                    {
                        //update position to start by parsing resetpositions
                        if (!float.TryParse(resetpositions.Split('|')[(dropround_i) * 3 + 0], out pos2startblockx))
                            Logger.Loginfo(DataType: "Event", LogMessage: "Unable to read x coordinate for reset position on watching pindrop");
                        if (!float.TryParse(resetpositions.Split('|')[(dropround_i) * 3 + 1], out pos2startblocky))
                            Logger.Loginfo(DataType: "Event", LogMessage: "Unable to read y coordinate for reset position on watching pindrop");
                        if (!float.TryParse(resetpositions.Split('|')[(dropround_i) * 3 + 2], out pos2startblockz))
                            Logger.Loginfo(DataType: "Event", LogMessage: "Unable to read z coordinate for reset position on watching pindrop");

                        dropround_i++;
                        if (dropround_i * 3 >= resetpositions.Split('|').Length)
                            dropround_i = 0;
                    }
                }
                
                InitCoinPoints();
                EnterRepositioning();
                break;
            case ObsRewardComm.TerminalCmds.T_SwapQuestion:
                decidingSurprise = true;
                otherSwapvoted = false;
                uitext.text = pindropResMsg;
                break;
            case ObsRewardComm.TerminalCmds.T_RequestPerfectReposition:
                //only the observer receives the request to reposition

                if (resetpositions.Contains('|'))
                {
                    if (resetpositions.Split('|').Length >= 3)
                    {
                        //update position to start by parsing resetpositions
                        if (!float.TryParse(resetpositions.Split('|')[(dropround_i) * 3 + 0], out pos2startblockx))
                            Logger.Loginfo(DataType: "Event", LogMessage: "Unable to read x coordinate for reset position on watching pindrop");
                        if (!float.TryParse(resetpositions.Split('|')[(dropround_i) * 3 + 1], out pos2startblocky))
                            Logger.Loginfo(DataType: "Event", LogMessage: "Unable to read y coordinate for reset position on watching pindrop");
                        if (!float.TryParse(resetpositions.Split('|')[(dropround_i) * 3 + 2], out pos2startblockz))
                            Logger.Loginfo(DataType: "Event", LogMessage: "Unable to read z coordinate for reset position on watching pindrop");
                        
                        dropround_i++;
                        if (dropround_i * 3 >= resetpositions.Split('|').Length)
                            dropround_i = 0;
                    }
                }

                mode2start = socialModes.watchingPinDropping;
                InitCoinPoints();
                EnterRepositioning();
                break;
        }
    }
    void BlockStart() {
        switch(mode2start){
            case socialModes.collectStandBy:
                uitext.text = getReady2CollectMsg;
                mode = mode2start;
                break;
            case socialModes.pindropStandBy:
                uitext.text = getReady2PindropMsg;
                mode = mode2start;
                break;
            case socialModes.watchCollectingStandBy:
                uitext.text = watchCollectGetReadyMsg;
                mode = mode2start;
                break;
            case socialModes.watchPinDroppingStandBy:
                uitext.text = watchPindropGetReadyMsg;
                mode = mode2start;
                break;
            case socialModes.watchingPinDropping:
                uitext.text = watchPindropMsg;
                mode = mode2start;
                break;
        }
        initialposIndicator.gameObject.SetActive(false);
    }
    IEnumerator HideLabels()
    {
        yield return new WaitForSeconds(10.0f);
        labelui.text = "";
    }
    IEnumerator ClearUI()
    {
        yield return new WaitForSeconds(uiDisplayTime);
        uitext.text = "";
    }
    IEnumerator ClearRewardTemp()
    {
        yield return new WaitForSeconds(rewardDisplayTime);
        rewardTempUI.text = "";
    }
    IEnumerator ClearRewardVis(float TimeFactor = 2.0f)
    {
        yield return new WaitForSeconds(TimeFactor*rewardDisplayTime);
        rewardUI.text = "";
    }
    IEnumerator ClearTotalRewardVis(float TimeFactor = 2.0f)
    {
        yield return new WaitForSeconds(TimeFactor * rewardDisplayTime);
        totalRewardUI.text = "";
    }
    IEnumerator DelayedDroppedPinsDisappear() {
        yield return new WaitForSeconds(2f * rewardDisplayTime);
        for (int i = 0; i < droppedPins.Count; i++)
        {
            Destroy(droppedPins[i]);
        }
        droppedPins.Clear();
    }
    void KeepLogging() {
        logtimer -= Time.deltaTime;
        if (logtimer <= 0f) {
            if (lograte == 0f)
                lograte = 50f;
            logtimer = 1f/lograte;

            headPosABase = persistentAnchor.persistentTarget.InverseTransformPoint(headTransform.position);
            headForthABase = persistentAnchor.persistentTarget.InverseTransformDirection(headTransform.forward);
            gazeABase = persistentAnchor.persistentTarget.InverseTransformDirection(Logger.gazeDir);
            fixationPointABase = persistentAnchor.persistentTarget.InverseTransformPoint(Logger.fixationPoint);

            //log info
            Logger.Loginfo(
                DataType: "RTdata",
                HeadPos:    headTransform.position.x.ToString("0.000") + " " +
                            headTransform.position.y.ToString("0.000") + " " +
                            headTransform.position.z.ToString("0.000"),
                HeadRot:    headTransform.rotation.eulerAngles.x.ToString("0.000") + " " +
                            headTransform.rotation.eulerAngles.y.ToString("0.000") + " " +
                            headTransform.rotation.eulerAngles.z.ToString("0.000"),
                EyeDirection:   Logger.gazeDir.x.ToString("0.000") + " " +
                                Logger.gazeDir.y.ToString("0.000") + " " +
                                Logger.gazeDir.z.ToString("0.000"),
                PupilLeft: Logger.leftPupilSize, PupilRight: Logger.rightPupilSize,
                EyeTarget: Logger.gazeTarget,
                HeadPosAnchored:    headPosABase.x.ToString("0.000") + " " +
                                    headPosABase.y.ToString("0.000") + " " +
                                    headPosABase.z.ToString("0.000"),
                    HeadForthAnchored:  headForthABase.x.ToString("0.000") + " " +
                                        headForthABase.y.ToString("0.000") + " " +
                                        headForthABase.z.ToString("0.000"),
                    EyeDirectionAnchored:   gazeABase.x.ToString("0.000") + " " +
                                            gazeABase.y.ToString("0.000") + " " +
                                            gazeABase.z.ToString("0.000"),
                    FixationPointAnchored:  fixationPointABase.x.ToString("0.000") + " " +
                                            fixationPointABase.y.ToString("0.000") + " " +
                                            fixationPointABase.z.ToString("0.000"),
                    OptiBodyApos:   optiBodyA.position.x.ToString("0.000") + " "+
                                    optiBodyA.position.y.ToString("0.000") + " " +
                                    optiBodyA.position.z.ToString("0.000"),
                    OptiBodyBpos:   optiBodyB.position.x.ToString("0.000") + " " +
                                    optiBodyB.position.y.ToString("0.000") + " " +
                                    optiBodyB.position.z.ToString("0.000"),
                    OptiBodyArot:   optiBodyA.rotation.x.ToString("0.000") + " " +
                                    optiBodyA.rotation.y.ToString("0.000") + " " +
                                    optiBodyA.rotation.z.ToString("0.000") + " " +
                                    optiBodyA.rotation.w.ToString("0.000"),
                    OptiBodyBrot:   optiBodyB.rotation.x.ToString("0.000") + " " +
                                    optiBodyB.rotation.y.ToString("0.000") + " " +
                                    optiBodyB.rotation.z.ToString("0.000") + " " +
                                    optiBodyB.rotation.w.ToString("0.000"),



                    OptiBodyBrot:   optiBodyB.rotation.x.ToString("0.000") + " " +
                                    optiBodyB.rotation.y.ToString("0.000") + " " +
                                    optiBodyB.rotation.z.ToString("0.000") + " " +
                                    optiBodyB.rotation.w.ToString("0.000"),

                    OptiBodyBrot:   optiBodyB.rotation.x.ToString("0.000") + " " +
                                    optiBodyB.rotation.y.ToString("0.000") + " " +
                                    optiBodyB.rotation.z.ToString("0.000") + " " +
                                    optiBodyB.rotation.w.ToString("0.000"),

                    OptiBodyBrot:   optiBodyB.rotation.x.ToString("0.000") + " " +
                                    optiBodyB.rotation.y.ToString("0.000") + " " +
                                    optiBodyB.rotation.z.ToString("0.000") + " " +
                                    optiBodyB.rotation.w.ToString("0.000"),

                    OptiBodyBrot:   optiBodyB.rotation.x.ToString("0.000") + " " +
                                    optiBodyB.rotation.y.ToString("0.000") + " " +
                                    optiBodyB.rotation.z.ToString("0.000") + " " +
                                    optiBodyB.rotation.w.ToString("0.000"),

                showmsg: false);
        }
    }
    void StartLog() {
        if (!Logger.fileopen) {
            char PartID = 'A';
            if (!mainPart)
                PartID = 'B';

            Logger.init(filenameCore + "_" + PartID);
            Logger.Loginfo(DataType: "Event", LogMessage: "Started Experiment");
            //log pathpoints
            LogCoinPoints();

            if(ObsRewardComm.HeadsetComm.useOptiObjs) {
                //adjust localipaddress
                motiveExample.localIPAddress = ObsRewardComm.HeadsetComm.ownIP;
                //enable and start optitrack feed
                optiBodyA.gameObject.SetActive(true);
                optiBodyB.gameObject.SetActive(true);
                motiveExample.ConnectFun();
            }
        }
    }
    
    void LogCoinPoints() {
        Logger.Loginfo(DataType: "Event", LogMessage: "coinsetID:"+coinsetID+" absolute and delta(local position)");
        for (int i = 0; i < pathPoints.Count; i++)
        {
            Logger.Loginfo(DataType: "Event", LogMessage: "coinpoint" + i + ":  x: " + pathPoints[i].position.x +
                                                                    " y: " + pathPoints[i].position.y +
                                                                    " z: " + pathPoints[i].position.z +
                                                                    " deltax:" + pathPoints[i].localPosition.x +
                                                                    " deltay:" + pathPoints[i].localPosition.y +
                                                                    " deltaz:" + pathPoints[i].localPosition.z);
        }
    }
    void OnApplicationQuit()
    {        
        CloseLog();
        ObsRewardComm.HeadsetComm.CloseComms();
    }
    void OnDestroy()
    {
        CloseLog();

        controllerActions.Bumper.performed -= HandleOnBumper;
        controllerActions.TriggerButton.performed -= HandleOnTrigger;
        controllerActions.Trigger.canceled -= HandleTriggerUp;

        ObsRewardComm.HeadsetComm.CloseComms();
    }
    void CloseLog()
    {
        if (Logger.fileopen)
        {
            logname = Logger.terminate();

            //send the logfile back
            //ObsRewardComm.HeadsetComm.SendLogFile2Terminal(logname);
        }        
    }
    void CutTaskShort() {
        switch (mode) {
            case socialModes.collecting:
                //make path invisible again
                ShowCoins(false);
                break;
            case socialModes.watchingCollection:
                //make the trail FX disappear
                trailObject.gameObject.SetActive(false);
                break;
            case socialModes.watchingPinDropping:
                //make the trail FX disappear
                trailObject.gameObject.SetActive(false);
                for (int i = 0; i < droppedPins.Count; i++) {
                    Destroy(droppedPins[i]);
                }
                droppedPins.Clear();
                break;
            case socialModes.pindropping:
                StartCoroutine(DelayedDroppedPinsDisappear());
                break;
            case socialModes.voting:
                VotingPanel.SetActive(false);
                controlPointer.gameObject.SetActive(false);
                break;
        }
    }
    public void SetReady() {
        //experimenter mode is the mode for the experimenter to setup the world anchor, which is persistent and only need to 
        //be done in one of the 2 expected devices to be connected with the terminal
        if (mode == socialModes.experimenterMode && !good2add)
        {
            persistentAnchor.anchorPlaced = true;

            //in case it auto snaped with the auto anchor, to make sure the anchor is persistent we need to publish it 
            persistentAnchor.RedefineAnchor(persistentAnchor.persistentTarget, false);

            //disable vuforia here
            Vuforia.VuforiaApplication.Instance.Deinit();

            //make anchor invisible
            anchorIndication.SetActive(false);

            //uitext.text = "ready to start...";
            EnablePathEdit(false);
            //save list of path points to the persistent file
            if (pathChanged) {
            #if !UNITY_EDITOR
                string pathFilePath = Path.Combine(Application.persistentDataPath,"CoinLocations.csv");
            #else
                string pathFilePath = Path.Combine(Application.streamingAssetsPath, "CoinLocations.csv");
            #endif
            
                FileStream pathstream = new FileStream(pathFilePath, FileMode.Create, FileAccess.ReadWrite);
                StreamWriter pathwriter = new StreamWriter(pathstream);
                for(int i=0;i<pathPoints.Count;i++) {
                    pathwriter.Write(coinsetID.ToString() + ",");
                    pathwriter.Write(pathPoints[i].localPosition.x.ToString("0.00")+",");
                    pathwriter.Write(pathPoints[i].localPosition.y.ToString("0.00")+",");
                    pathwriter.Write(pathPoints[i].localPosition.z.ToString("0.00"));
                    if(i<pathPoints.Count-1)
                        pathwriter.Write("\n");
                }
                pathwriter.Close();
                pathstream.Close();
            }

            //set mode to wait for terminal connection
            mode = socialModes.ready4Connection;
            Debug.Log("Right before the init, ppoints size is:" + ppointsx.Count);
            //Debug.Log("PCF ID first:"+persistentAnchor.anchorBinding.PCF.CFUID.First);
            //Debug.Log("PCF ID sec:" + persistentAnchor.anchorBinding.PCF.CFUID.Second);

            rewardUI.text = "";
            uitext.text = "ready to connect to terminal";
            StartCoroutine(ClearUI());
            
            //calculating relative headpos in terms of the pcf
            Matrix4x4 anchorCoordinateSpace = Matrix4x4.TRS(persistentAnchor.persistentTarget.position, persistentAnchor.persistentTarget.rotation, Vector3.one);
            Vector3 relativeHeadPos = Matrix4x4.Inverse(anchorCoordinateSpace).MultiplyPoint3x4(headTransform.position);

            //prepare the HeadSetComm and connect to the terminal, that is when the comm thread is started
            ObsRewardComm.HeadsetComm.init(this, mainPart,
                        relativeHeadPos.x,
                        relativeHeadPos.y,
                        relativeHeadPos.z,
                pathids,
                ppointsx, ppointsy, ppointsz,coinValues);
            

            //open socket threads to wait for the terminal connection
            mode = socialModes.connectedIdle;
        }
    }
    void ShowCoins(bool Activate)
    {
        //pathEditing = Activate;
        //pathLine.positionCount = pathPoints.Count;
        //Vector3[] points = new Vector3[pathPoints.Count];
        for (int i = 0; i < pathPoints.Count; i++)
        {
            pathPoints[i].gameObject.SetActive(Activate);
            //points[i] = new Vector3(pathPoints[i].position.x, pathTransform.position.y, pathPoints[i].position.z);
        }
        //pathLine.SetPositions(points);
        //pathLine.enabled = Activate;
    }
    void EnablePathEdit(bool Activate){
        pathEditing = Activate;
        ShowCoins(Activate);
        newPathPointIcon.gameObject.SetActive(Activate);
        controlPointer.gameObject.SetActive(Activate);
    }
    public void SelectPathPoint(Transform PointTarget) {
        PointTarget.GetComponent<Renderer>().material = selectedMatPathPoint;
        PointTarget.GetChild(0).gameObject.SetActive(false);
        PointTarget.GetChild(1).gameObject.SetActive(true);
    }
    public void UnselectPathPoint(Transform PointTarget)
    {
        PointTarget.GetComponent<Renderer>().material = normalMatPathPoint;
        PointTarget.GetChild(0).gameObject.SetActive(true);
        PointTarget.GetChild(1).gameObject.SetActive(false);
    }
    void RemovePointAt(Transform Target) {
        //because now the collider is on the child, not on the pathpoint gameobject itself
        Target = Target.parent;
        if (pathPoints.Contains(Target))
        {
            //delete point
            Destroy(Target.gameObject);
            pathPoints.Remove(Target);
            ShowCoins(false);            
            if (pathPointsel > pathPoints.Count - 1)
                pathPointsel = 0;
            pathChanged = true;
        }
    }
    void AddCoinPoint(Vector3 Position2Add, float CoinValue, bool Changed=true, bool IsLocal=false) {
        //add point
        GameObject newpoint = GameObject.Instantiate(collectableCoinPrefab, pathTransform);
        if (IsLocal) {
            newpoint.transform.localPosition = Position2Add;
        }
        else
        {
            newpoint.transform.position = Position2Add;
        }        

        CoinValue thiscoin = newpoint.GetComponent<CoinValue>();
        thiscoin.coinValue = CoinValue;
        thiscoin.openSpeed = openChestSpeed;
        thiscoin.revealDelay = delayedRevealTime;
        thiscoin.specialThreshold = specialThreshold;
        thiscoin.SetupAudio(maudio, rewardSound, penaltySound);

        //deactivating collider if it is an observer
        if (mode == socialModes.watchingCollection)
        {
            Collider thiscol = thiscoin.closedLock.GetComponent<Collider>();
            if (thiscol)
                thiscol.enabled = false;
            else
                Debug.Log("[TaskMan] Failed to deactivate coin collider in observer.");

            thiscoin.headTransform = trailObject;
        }

        pathPoints.Add(newpoint.transform);
        ShowCoins(false);
        pathChanged = Changed;
    }
    public void EditPath(Transform Target2Remove) {
        //if in editpath mode, it might add or remove a point
        if (mode == socialModes.experimenterMode && pathEditing) {
            if (Target2Remove)
            {
                RemovePointAt(Target2Remove);
            }
            else
            {
                if(good2add)
                    AddCoinPoint(addpointpos,1);
            }
        }
    }
    void HandleOnBumper(InputAction.CallbackContext obj)
    {
        lastTriggerTime = -1f;
        if (controllerActions.Trigger.ReadValue<float>() > 0.5f)
        {
            if (quitTimerStart < 0f)
            {
                quitTimerStart = Time.time;
                /*
                //testing the manual function
                UnityEngine.XR.MagicLeap.Native.MagicLeapNativeBindings.MLCoordinateFrameUID CFUID;
                CFUID = new UnityEngine.XR.MagicLeap.Native.MagicLeapNativeBindings.MLCoordinateFrameUID();
                CFUID.First = persistentAnchor.anchorBinding.PCF.CFUID.First;
                CFUID.Second = persistentAnchor.anchorBinding.PCF.CFUID.Second;
                Debug.Log("my cfuid:"+ CFUID.ToGuid());
                Debug.Log("original:" + persistentAnchor.anchorBinding.PCF.CFUID.ToGuid());
                persistentAnchor.ManualPCFBind(CFUID, Vector3.zero, Quaternion.identity);
                */
            }
            else
            {
                if (Time.time - quitTimerStart < 0.5f)
                {
                    if (mode != socialModes.experimenterMode)
                    {
                        CloseLog();
                        ObsRewardComm.HeadsetComm.Ask2Quit();
                    }
                    Application.Quit();
                }
                else
                {
                    quitTimerStart = -1f;
                }
            }
        }
        else {
            quitTimerStart = -1f;
            if (decidingSurprise)
            {
                if(mode == socialModes.watchingPinDropping) {
                    uitext.text = "";
                    decidingSurprise = false;
                    Logger.Loginfo(DataType: "Event", LogMessage: "Observer says it was NOT a swaped round.");

                    rewardUI.text = "AN: $" + reward.ToString("0.00");
                    StartCoroutine(ClearRewardVis(1.5f));

                    totalRewardUI.text = "$" + totalReward.ToString("0.00");
                    StartCoroutine(ClearTotalRewardVis(1.5f));

                    if(otherSwapvoted){
                        FinishPindropWatching(true);
                    }
                }
                else {
                    uitext.text = "";
                    decidingSurprise = false;
                    Logger.Loginfo(DataType: "Event", LogMessage: "Active Navigator says it was NOT a swaped round.");

                    SecondStepReachedCoinFinishRound();
                }
            }
            else
            {
                if (mode == socialModes.experimenterMode)
                {
                    persistentAnchor.RedefineAnchor(controlTransform);
                }
                else if (mode == socialModes.pindropVoting)
                {
                    //register the positive vote
                    DroppedPinVote(1);
                }
            }
        }
    }
    
    void HandleOnTrigger(InputAction.CallbackContext obj)
    {
        if (decidingSurprise){
            if(mode == socialModes.watchingPinDropping) {
                uitext.text = "";
                decidingSurprise = false;
                Logger.Loginfo(DataType: "Event", LogMessage: "Observer says it was a swaped round.");

                rewardUI.text = "AN: $" + reward.ToString("0.00");
                StartCoroutine(ClearRewardVis(1.5f));

                totalRewardUI.text = "$" + totalReward.ToString("0.00");
                StartCoroutine(ClearTotalRewardVis(1.5f));

                if (otherSwapvoted)
                {
                    FinishPindropWatching(true);
                }
            }
            else {
                uitext.text = "";
                decidingSurprise = false;
                Logger.Loginfo(DataType: "Event", LogMessage: "Active Navigator says it was a swaped round.");
                
                SecondStepReachedCoinFinishRound();
            }
        }
        else
        {
            switch (mode)
            {
                case socialModes.experimenterMode:
                    if (lastTriggerTime < 0f)
                    {
                        lastTriggerTime = Time.time;
                        EditPath(pathPointTransform);
                    }
                    else
                    {
                        if (Time.time - lastTriggerTime < 0.5f)
                        {
                            SetReady();
                        }
                        else
                        {
                            lastTriggerTime = Time.time;
                            EditPath(pathPointTransform);
                        }
                    }
                    break;
                case socialModes.collectStandBy:
                    //StartObservedTrail(false);
                    //send message to terminal to change other participant mode from watch standby to watching
                    //UpdateCurrPosNSend();
                    StartCollecting();
                    ObsRewardComm.HeadsetComm.UpdateStatus(ObsRewardComm.TerminalCmds.T_FinishedCurrentTask, "starting coin collecting");
                    break;
                case socialModes.pindropStandBy:
                    //StartObservedTrail(true);
                    //send message to terminal to change other participant mode from watch standby to watching
                    //UpdateCurrPosNSend();
                    StartPinDropping();

                    if(dropround == rec_dropround) {
                        ObsRewardComm.HeadsetComm.UpdateStatus(ObsRewardComm.TerminalCmds.T_FinishedCurrentTask, "started pin dropping");
                    }
                    
                    break;
                case socialModes.pindropping:
                    Logger.Loginfo(DataType: "Event", LogMessage: "Just dropped a pin.");
                    mode = socialModes.pindropWalk2Coin;                    

                    maudio.PlayOneShot(pindropSound);
                    Vector3 newpinpos = anchorIndication.transform.position + Vector3.ProjectOnPlane(controlTransform.position - anchorIndication.transform.position, Vector3.up);
                    GameObject newpin = GameObject.Instantiate(pinGO, newpinpos, Quaternion.identity);
                    droppedPins.Add(newpin);
                    
                    Vector3 localpinpos = persistentAnchor.persistentTarget.InverseTransformPoint(newpinpos);

                    //log pindrop position
                    Logger.Loginfo(DataType: "Event", LogMessage: "Dropped a new pin at " + 
                        newpinpos.x.ToString("0.000")+" "+
                        newpinpos.y.ToString("0.000")+" "+
                        newpinpos.z.ToString("0.000")+" localpos: "+    localpinpos.x.ToString("0.000")+" "+ 
                                                                        localpinpos.y.ToString("0.000")+" "+ 
                                                                        localpinpos.z.ToString("0.000"));

                    //find closest chest
                    float closestdist = -1f;
                    int closest_i = 0;
                    for (int i = 0; i < pathPoints.Count; i++)
                    {
                        float thisdist = (pathPoints[i].position - newpinpos).magnitude;
                        if (closestdist < 0f || thisdist < closestdist)
                        {
                            closestdist = thisdist;
                            closest_i = i;
                        }
                    }

                    float thiscoinvalue = pathPoints[closest_i].GetComponent<CoinValue>().coinValue;

                    string newpindropmsg = "Dropped a whichdrop pin";
                    string newlogmsg = "Closest location was: coinloc | actual distance: closestdist | whichdrop drop | coinValue: coinvvalue";
                    newlogmsg = newlogmsg.Replace("coinloc", pathPoints[closest_i].localPosition.x.ToString("0.000")+ pathPoints[closest_i].localPosition.y.ToString("0.000")+ pathPoints[closest_i].localPosition.z.ToString("0.000"));
                    newlogmsg = newlogmsg.Replace("closestdist", closestdist.ToString("0.000"));
                    newlogmsg = newlogmsg.Replace("coinvvalue", thiscoinvalue.ToString("0.00"));

                    //give distance feedback
                    if (closestdist < scoringDist)
                    {
                        newlogmsg = newlogmsg.Replace("whichdrop", "good");
                        newpindropmsg = newpindropmsg.Replace("whichdrop", "good");
                        StartCoroutine(DelayedPinFeedback(newpin.transform.GetChild(0).GetComponent<Renderer>(), true, closest_i));
                        lastdropgood = true;
                    }
                    else
                    {
                        newlogmsg = newlogmsg.Replace("whichdrop", "bad");
                        newpindropmsg = newpindropmsg.Replace("whichdrop", "bad");
                        StartCoroutine(DelayedPinFeedback(newpin.transform.GetChild(0).GetComponent<Renderer>(), false, closest_i));
                        perfectDroprun = false;
                        perfectruns = 0;
                        reward = 0;                        
                        lastdropgood = false;
                    }
                    int fullGoodRounds = perfectruns;
                    if (perfectDroprun && nextcoin_i >= coinAmount - 1)
                        fullGoodRounds = perfectruns + 1;

                    //reward value to send to terminal
                    float futureReward = validatedReward;
                    float futureTotalReward = totalReward;                    
                    if (lastdropgood && perfectDroprun)
                    {
                        if(nextcoin_i<2) {
                            futureReward += 2 * thiscoinvalue;
                        }
                        else {
                            futureReward += thiscoinvalue;
                        }

                        //checking if last pindrop
                        if(nextcoin_i >= coinAmount - 1)
                        {
                            if(fullGoodRounds>= perfectRoundsTarget)
                                futureTotalReward += futureReward + validatedReward;
                        }
                    }
                    newpindropmsg += "|"+dropround+"|"+fullGoodRounds+"|"+ futureReward.ToString("0.00")+"|"+ futureTotalReward.ToString("0.00");

                    Logger.Loginfo(DataType: "Event", LogMessage: newlogmsg);
                    Logger.Loginfo(DataType: "Event", LogMessage: newpindropmsg);

                    //send it to the other participant
                    SendNewPinDrop(newpinpos, newpindropmsg);
                    break;
                case socialModes.watchingCollection:
                    if (nextcoin_i >= coinAmount)
                    {
                        Logger.Loginfo(DataType: "Event", LogMessage: "Finished watching other participant's collection.");
                        uitext.text = finishWatchCollectionMsg;
                        trailObject.gameObject.SetActive(false);
                        //finish watching
                        FinishedCurrentTask();
                    }
                    break;
                case socialModes.watchingPinDropping:
                    if (droppedPins.Count < coinAmount)
                    {
                        uitext.text = notyetfinishedWatching;
                        StartCoroutine(ClearUI());
                    }
                    else
                    {
                        Logger.Loginfo(DataType: "Event", LogMessage: "Finished watching other participant's pindropping.");
                        uitext.text = finishWatchPinDroppingMsg;
                        trailObject.gameObject.SetActive(false);
                        //finish watching
                        FinishedCurrentTask();
                    }
                    break;
                case socialModes.voting:
                    if (voteButton)
                    {
                        Vote(voteButton.name.Replace("Vote", ""));
                    }
                    break;
                case socialModes.pindropVoting:
                    //register the negative vote
                    DroppedPinVote(-1);
                    break;
            }
        }
    }
    private void HandleTriggerUp(InputAction.CallbackContext obj)
    {
        if (mode == socialModes.experimenterMode)
        {
            if (lastTriggerTime > 0f)
            {
                //user needs to hold it for 2seconds or more
                if (Time.time - lastTriggerTime >= 2f)
                {
                    EnablePathEdit(true);
                }
            }
        }
    }
    void StartCollecting() {
        Logger.globalBlock++;
        Logger.Loginfo(DataType: "Event", LogMessage: "Started collecting. Block:" + taskBlock_i);
        //check if in the middle of a task
        if (mode != socialModes.connectedIdle)
            CutTaskShort();
        uitext.text = startCollectingMsg;
        StartCoroutine(ClearUI());
        mode = socialModes.collecting;
        nextcoin_i = 0;
        StartLog();

        InitCoinPoints();
        LogCoinPoints();
        pathPoints[nextcoin_i].gameObject.SetActive(true);
    }
    void StartPinDropping() {
        Logger.Loginfo(DataType: "Event", LogMessage: "Started pindropping. Block:" + taskBlock_i);
        //perfectruns = 0;
        //dropround = 1;
        perfectDroprun = true;
        Logger.globalBlock++;
        nextcoin_i = 0;
        //if (mode != socialModes.connectedIdle)
        //    CutTaskShort();

        InitCoinPoints();        

        mode = socialModes.pindropping;
        for (int i = 0; i < droppedPins.Count; i++)
        {
            Destroy(droppedPins[i]);
        }
        droppedPins.Clear();
        
        uitext.text = pindropMsg;
        StartCoroutine(ClearUI());
    }
    public void StartObservedTrail(Vector3 OtherPos) {
        StartLog();

        trailObject.gameObject.SetActive(true);
        trailObject.position = OtherPos;
        AdjustFakeTrailBody(trailObject.position.y);
        #if UNITY_EDITOR
            nextcoin_i = 0;
            newtrailpos = pathPoints[nextcoin_i].position;
        #endif
    }
    void StartVoting() {        
        VotingPanel.SetActive(true);
        trailObject.gameObject.SetActive(false);
        Logger.globalBlock++;
        Logger.Loginfo(DataType: "Event", LogMessage: "started voting");
        mode = socialModes.voting;

        uitext.text = "voting";
        StartCoroutine(ClearUI());
    }
    /*
    public void StartIdle() {
        //function created in case the participant needs to wait for the other participant
        mode = socialModes.connectedIdle;
        Logger.Loginfo(DataType: "Event", LogMessage: "started idle wait", showmsg: true);
    }
    */
    public void Vote(string Vote) {
        Logger.Loginfo(DataType: "Event", LogMessage: "voted:" + Vote, showmsg: true);
        VotingPanel.SetActive(false);
        controlPointer.gameObject.SetActive(false);
        uitext.text = "Vote registered. Standby";

        int score2send = 0;
        if (!int.TryParse(Vote, out score2send))
            Debug.Log("[TaskMan] Could not interpret user's vote");

        ObsRewardComm.HeadsetComm.SendScore(score2send);
        FinishedCurrentTask();
    }
    /*
    public void UpdateAnchorGeo(    ulong CfuidFirst, ulong CfuidSec,
                                    float OtherPartAnchorx,
                                    float OtherPartAnchory,
                                    float OtherPartAnchorz,
                                    float OtherPartAnchorfx,
                                    float OtherPartAnchorfy,
                                    float OtherPartAnchorfz,
                                    float OtherPartAnchorfw) {
        otherUIDfirst = CfuidFirst;
        otherUIDsec = CfuidSec;
        otherPartAnchorx = OtherPartAnchorx;
        otherPartAnchory = OtherPartAnchory;
        otherPartAnchorz = OtherPartAnchorz;
        otherPartAnchorfx = OtherPartAnchorfx;
        otherPartAnchorfy = OtherPartAnchorfy;
        otherPartAnchorfz = OtherPartAnchorfz;
        otherPartAnchorfw = OtherPartAnchorfw;
    }
    */
    public void UpdatePathPoints(List<int> Pathids,List<float> PPointsx, List<float> PPointsy, List<float> PPointsz, List<float> CoinValues) {
        pathids = Pathids;
        ppointsx = PPointsx;
        ppointsy = PPointsy;
        ppointsz = PPointsz;
        coinValues = CoinValues;
    }
    public void UpdateOtherParticipantRawPos(float OtherpartRawx, float OtherpartRawy, float OtherpartRawz) {
        otherpartRawx = OtherpartRawx;
        otherpartRawy = OtherpartRawy;
        otherpartRawz = OtherpartRawz;
    }
    void UpdateOtherParticipantPos() {
        //Function intended to update the other participant position so we can create a real time trail FX on the ground
        //calculate back to world pos
        Vector3 relPosition = new Vector3(otherpartRawx, otherpartRawy, otherpartRawz);
        Matrix4x4 anchorCoordinateSpace = new Matrix4x4();
        anchorCoordinateSpace.SetTRS(persistentAnchor.persistentTarget.position, persistentAnchor.persistentTarget.rotation, Vector3.one);

        Vector3 newpos = anchorCoordinateSpace.MultiplyPoint3x4(relPosition);

        otherpartx = newpos.x;
        otherparty = newpos.y;
        otherpartz = newpos.z;
    }
    public void UpdateOtherPinDropRaw(float OtherpindropRawx, float OtherpindropRawy, float OtherpindropRawz) {
        otherpindropRawx = OtherpindropRawx;
        otherpindropRawy = OtherpindropRawy;
        otherpindropRawz = OtherpindropRawz;
    }
    void UpdateOtherPinDrop()
    {
        //Function intended to update the other participant position so we can create a real time trail FX on the ground
        //calculate back to world pos
        Vector3 relPosition = new Vector3(otherpindropRawx, otherpindropRawy, otherpindropRawz);
        Matrix4x4 anchorCoordinateSpace = new Matrix4x4();
        anchorCoordinateSpace.SetTRS(persistentAnchor.persistentTarget.position, persistentAnchor.persistentTarget.rotation, Vector3.one);

        Vector3 newpos = anchorCoordinateSpace.MultiplyPoint3x4(relPosition);

        otherpindropx = newpos.x;
        otherpindropy = newpos.y;
        otherpindropz = newpos.z;
    }

    void UpdateCurrPosNSend()
    {
        //calculating relative headpos in terms of the anchor
        Matrix4x4 anchorCoordinateSpace = Matrix4x4.TRS(persistentAnchor.persistentTarget.position, persistentAnchor.persistentTarget.rotation, Vector3.one);
        Vector3 relativeHeadPos = Matrix4x4.Inverse(anchorCoordinateSpace).MultiplyPoint3x4(headTransform.position);
        ObsRewardComm.HeadsetComm.UpdateCurrPos(relativeHeadPos.x,
                                                relativeHeadPos.y,
                                                relativeHeadPos.z);
        ObsRewardComm.HeadsetComm.UpdateStatus(ObsRewardComm.TerminalCmds.T_NewWatchedPos, "position update");
    }
    void SendNewPinDrop(Vector3 PindropPos, string DropString)
    {
        //calculating relative headpos in terms of the anchor
        Matrix4x4 anchorCoordinateSpace = Matrix4x4.TRS(persistentAnchor.persistentTarget.position, persistentAnchor.persistentTarget.rotation, Vector3.one);
        Vector3 relativePinPos = Matrix4x4.Inverse(anchorCoordinateSpace).MultiplyPoint3x4(PindropPos);
        ObsRewardComm.HeadsetComm.UpdateNewPinDropPoint(relativePinPos.x,
                                                        relativePinPos.y,
                                                        relativePinPos.z);
        ObsRewardComm.HeadsetComm.UpdateStatus(ObsRewardComm.TerminalCmds.T_PinDrop, DropString);
    }
    void AdjustFakeTrailBody(float Fheight) {
        //adjust height of the fakebody, we add .25 because the point of reference is actually the center of the head
        float realHeight = Fheight-anchorIndication.transform.position.y;
        trailFakeBodyScale.y = (realHeight + 0.25f) / 2;
        trailFakeBodyPos.y = -(realHeight - 0.25f) / 2;
        trailFakeBody.localScale = trailFakeBodyScale;
        trailFakeBody.localPosition = trailFakeBodyPos;
        trailObject.gameObject.SetActive(true);
    }
    void InitCoinPoints() {
        if (pathids.Count<=0) {
            Debug.Log("Coinpoints not available! Using dummy from the scene file.");
        }
        else {
            for (int i = 0; i < pathPoints.Count; i++)
            {
                Destroy(pathPoints[i].gameObject);
            }
            pathPoints.Clear();
            for (int i=0;i<pathids.Count;i++) {
                if (coinsetID == pathids[i]) {
                    Vector3 newppoint = new Vector3(ppointsx[i],ppointsy[i], ppointsz[i]);
                    AddCoinPoint(newppoint, coinValues[i], false, true);
                }
            }
        }
        ShowCoins(false);
        coinAmount = pathPoints.Count;
    }    
    
    void SetMainParticipant(bool MainPart) {
        mainPart = MainPart;
    }
    public void FinishedCurrentTask() {        
        Logger.Loginfo(DataType: "Event", LogMessage: "finished current task", showmsg: true);
        CutTaskShort();

        mode = socialModes.connectedIdle;
        StartCoroutine(ClearRewardVis());
        //update the terminal
        //also send current pos so terminal always knows where participant was when became idle
        //UpdateCurrPosNSend();
        ObsRewardComm.HeadsetComm.UpdateStatus(ObsRewardComm.TerminalCmds.T_FinishedCurrentTask, "finished current task");
    }
    public void NewCommMsg(string message)
    {
        msgcommstr = message;
        newcommmsg = true;
    }
    void OnTriggerEnter(Collider other)
    {
        //collect the coin
        if (other.tag == "Finish")
        {
            //this should be a feedbackcoin
            FeedbackCoin fvalue = other.transform.GetComponent<FeedbackCoin>();            
            if (fvalue)
            {
                //Destroy feedback coin
                Destroy(other.gameObject);

                //hide the previous droppedpin
                droppedPins[droppedPins.Count - 1].gameObject.SetActive(false);

                Logger.Loginfo(DataType: "Event", LogMessage: "Collected pin feedback coin: " + nextcoin_i, showmsg: true);
                //send flag to terminal so observer gets updated too
                ObsRewardComm.HeadsetComm.UpdateStatus(ObsRewardComm.TerminalCmds.T_FeedbackCoinCollected, "Collected pin feedback coin: " + nextcoin_i);

                ReachedFeedbackCoin(fvalue.coinValue);
            }
            else
            {
                //this should be a Chest lock
                CoinValue cvalue = other.transform.parent.GetComponent<CoinValue>();
                if (cvalue) {
                    maudio.PlayOneShot(coinCollectionSound);
                    cvalue.OpenChest(false, this);
                    Logger.Loginfo(DataType: "Event", LogMessage: "Chest opened: " + nextcoin_i, showmsg: true);
                    ObsRewardComm.HeadsetComm.UpdateStatus(ObsRewardComm.TerminalCmds.T_ChestOpened, "Chest opened: " + nextcoin_i);
                }
                else {
                    Debug.Log("<color=red>Found coin but it lacks both the CoinValue and the FeedbackCoin scripts</color>");
                }
            }
        }
        else {
            Debug.Log("<color=red>Collided with something that is not a coin: "+other.name+"</color>");
        }
    }
    public void UpdateUIReward(float CoinValue) {
        
        reward += CoinValue;
        rewardUI.text = "$" + reward.ToString("0.00");
        rewardTempUI.text = "$" + CoinValue.ToString("0.00");
        StartCoroutine(ClearRewardTemp());

        Logger.Loginfo(DataType: "Event", LogMessage: "coin collected: " + nextcoin_i, showmsg: true);
        //send update message for the observer
        ObsRewardComm.HeadsetComm.UpdateStatus(ObsRewardComm.TerminalCmds.T_CollectedCoin, "coin collected: " + nextcoin_i);
    }
    public void NextCollection(){
        pathPoints[nextcoin_i].gameObject.SetActive(false);

        //update displayed coin
        if(nextcoin_i < coinAmount-1)
        {
            nextcoin_i++;
            pathPoints[nextcoin_i].gameObject.SetActive(true);
            activeChest = pathPoints[nextcoin_i].gameObject.GetComponent<CoinValue>();

            if (mode == socialModes.watchingCollection) {
                uitext.text = "coin collected";
                StartCoroutine(ClearUI());
            }
        }
        else
        {
            if (mode == socialModes.collecting)
            {
                //wait for next phase
                uitext.text = finishCollectionMsg;
                FinishedCurrentTask();
                nextcoin_i = 0;
            }
            else if (mode == socialModes.watchingCollection) {
                trailObject.gameObject.SetActive(false);
                uitext.text = endOfroundMsg;
                nextcoin_i++;
            }
        }
    }
    IEnumerator DelayedPinFeedback(Renderer PinTarget, bool CorrectPlacement, int Closest_i) {
        yield return new WaitForSeconds(time4pinfeedback);
        Vector3 coinpos = pathPoints[Closest_i].position + 1.2f * Vector3.up;
        float thisvalue = pathPoints[Closest_i].GetComponent<CoinValue>().coinValue;
        if (CorrectPlacement) {
            PinTarget.material = goodPinMat;
            uitext.text = "correct";
        }
        else {
            uitext.text = "incorrect";
            PinTarget.material = badPinMat;
        }

        //remove this point to avoid repetition
        Destroy(pathPoints[Closest_i].gameObject);
        pathPoints.RemoveAt(Closest_i);

        yield return new WaitForSeconds(feedbackCoinDelay);

        //spawn corresponding feedback coin, which needs to be collected to call  ReachedFeedbackCoin();
        GameObject afeedbackcoin;
        
        if (thisvalue < 0.0001f) {
            afeedbackcoin = Instantiate(fdbkBadcoin);
        }
        else if (thisvalue < specialThreshold) {
            afeedbackcoin = Instantiate(fdbkGoodcoin);
        }
        else {
            afeedbackcoin = Instantiate(fdbkSpecialcoin);
        }
        afeedbackcoin.transform.position = coinpos;
        FeedbackCoin thisfcoin = afeedbackcoin.GetComponent<FeedbackCoin>();
        if (thisfcoin)
            thisfcoin.coinValue = thisvalue;
        else
            Debug.Log("<color=red>[TaskMan] Very weird: feedback coin is missing FeedbackCoin script, probably wrong prefab assigned in the scene</color>");
        
        yield return new WaitForSeconds(time4pinfeedback);
    }
    void ReachedFeedbackCoin(float CoinValue) {
        if (CoinValue < 0.001f)
        {
            maudio.PlayOneShot(penaltySound);
        }
        else if (CoinValue < specialThreshold)
        {
            maudio.PlayOneShot(penaltySound);
        }
        else
        {
            maudio.PlayOneShot(rewardSound);
        }

        if(decidingSurprise) {
            Logger.Loginfo(DataType: "Event", LogMessage: "A.N. did not vote for the swaped round.");
        }
        decidingSurprise = false;
        mode = socialModes.pindropping;
        float tempReward = 0f;
        if (lastdropgood && perfectDroprun)
        {
            if (nextcoin_i < 2)
            {
                tempReward = 2 * CoinValue;
            }
            else { 
                tempReward = CoinValue;
            }
            reward += tempReward;
            //rewardUI.text = "$" + reward.ToString("0.00");
        }
        else {
            tempReward = CoinValue; 
            reward = 0f; 
        }

        rewardTempUI.text = "$" + tempReward.ToString("0.00");
        StartCoroutine(ClearRewardTemp());
        Logger.Loginfo(DataType: "Event", LogMessage: "Collected feedback coin:" + tempReward.ToString("0.00") + " total reward: "+reward.ToString("0.00"));
        
        if (nextcoin_i < coinAmount-1)
        {
            nextcoin_i++;
            uitext.text = pindropMsg;
            StartCoroutine(ClearUI());
        }
        else
        {
            if(perfectRoundsTarget < 1) {
                //allow A.N. to vote if that result was surprising
                decidingSurprise = true;
                uitext.text = pindropResMsg;

                //to avoid AN from seeing the totals
                rewardUI.text = "";
                totalRewardUI.text = "";

                //allow Observer to vote
                ObsRewardComm.HeadsetComm.UpdateStatus(ObsRewardComm.TerminalCmds.T_SwapQuestion, "Swap question");
            }
            else {
                SecondStepReachedCoinFinishRound();
                //to avoid AN from seeing the totals
                rewardUI.text = "";
                totalRewardUI.text = "";
            }
        }
    }
    void SecondStepReachedCoinFinishRound() {
        Logger.Loginfo(DataType: "Event", LogMessage: "Finished pindrop round:" + dropround);

        dropround++;

        //update reposition if possible
        if (resetpositions.Contains('|'))
        {
            if (resetpositions.Split('|').Length >= 3)
            {
                if (!float.TryParse(resetpositions.Split('|')[(dropround_i) * 3 + 0], out pos2startblockx))
                    Logger.Loginfo(DataType: "Event", LogMessage: "Unable to read x coordinate for reset position on pindroping");
                if (!float.TryParse(resetpositions.Split('|')[(dropround_i) * 3 + 1], out pos2startblocky))
                    Logger.Loginfo(DataType: "Event", LogMessage: "Unable to read y coordinate for reset position on pindroping");
                if (!float.TryParse(resetpositions.Split('|')[(dropround_i) * 3 + 2], out pos2startblockz))
                    Logger.Loginfo(DataType: "Event", LogMessage: "Unable to read z coordinate for reset position on pindroping");

                dropround_i++;
                if (dropround_i * 3 >= resetpositions.Split('|').Length)
                    dropround_i = 0;
            }
        }

        if (perfectDroprun || perfectRoundsTarget == 0)
        {
            rewardUI.text = "$" + reward.ToString("0.00");
            StartCoroutine(ClearRewardVis(1.5f));

            validatedReward += reward;
            perfectruns++;
            if (perfectruns >= perfectRoundsTarget)
            {
                totalReward += validatedReward;               

                //finish pin dropping
                uitext.text = finishDroppingMsg;
                FinishedCurrentTask();
            }
            else
            {
                //starting next round
                nextcoin_i = 0;
                uitext.text = pindropMsg2;
                //StartCoroutine(ClearUI());
                for (int i = 0; i < droppedPins.Count; i++)
                {
                    Destroy(droppedPins[i]);
                }
                droppedPins.Clear();

                mode2start = socialModes.pindropStandBy;
                EnterRepositioning();
                //ask the observer to reposition
                ObsRewardComm.HeadsetComm.UpdateStatus(ObsRewardComm.TerminalCmds.T_RequestPerfectReposition, "");
            }

            totalRewardUI.text = "$" + totalReward.ToString("0.00");
            StartCoroutine(ClearTotalRewardVis(1.5f));

            Logger.Loginfo(DataType: "Event", LogMessage: "Finished a perfect round with:" + reward.ToString("0.00") + " total reward: " + totalReward.ToString("0.00"));
            reward = 0f;
        }
        else
        {
            //reset runs
            perfectDroprun = true;
            perfectruns = 0;
            validatedReward = 0f;
            nextcoin_i = 0;
            reward = 0f;
            //rewardUI.text = "$" + reward.ToString("0.00");
            uitext.text = pindropMsg2;
            //StartCoroutine(ClearUI());
            for (int i = 0; i < droppedPins.Count; i++)
            {
                Destroy(droppedPins[i]);
            }
            droppedPins.Clear();
            mode2start = socialModes.pindropStandBy;
            EnterRepositioning();
            ObsRewardComm.HeadsetComm.UpdateStatus(ObsRewardComm.TerminalCmds.T_RequestReposition, "");
        }
    }

    void DroppedPinVote(int ChosenCorrect) {
        //hide pindrop voting timer indicator
        timer4pindropVote_ui.enabled = false;
        uitext.text = "";
        mode = socialModes.watchingPinDropping;
        if (ChosenCorrect == 0) {
            //user did not vote, auto vote
            Logger.Loginfo(DataType: "Event", LogMessage: "Observer did not vote for this pindrop from the navigator", showmsg: true);
        }
        else if (ChosenCorrect > 0)
        {
            Logger.Loginfo(DataType: "Event", LogMessage: "Observer chose CORRECT for this pindrop from the navigator", showmsg: true);
        }
        else {
            Logger.Loginfo(DataType: "Event", LogMessage: "Observer chose INCORRECT for this pindrop from the navigator", showmsg: true);
        }

        //instantiate feedback coin        
        if (ChosenCorrect != 0) {
            if ((ChosenCorrect > 0 && closestdist < scoringDist) || (ChosenCorrect < 0 && closestdist > scoringDist))
            {
                //observedFeedbackcoin = Instantiate(fdbkGoodcoin);
                //change pin color
                observedPinRender.material = goodPinMat;
                uitext.text = "correct";
                StartCoroutine(ClearUI());
            }
            else
            {
                //observedFeedbackcoin = Instantiate(fdbkSpecialcoin);
                observedPinRender.material = badPinMat;
                uitext.text = "incorrect";
                StartCoroutine(ClearUI());
            } 
        }
        StartCoroutine(DelayedFeedbackCoin());
    }
    IEnumerator DelayedFeedbackCoin(){
        //feedback coin for the observer
        yield return new WaitForSeconds(feedbackCoinDelay);

        if (!observedCoinAlreadyCollected)
        {
            if (observedCoinValue < 0.0001f)
            {
                observedFeedbackcoin = Instantiate(fdbkBadcoin);
            }
            else if (observedCoinValue < specialThreshold)
            {
                observedFeedbackcoin = Instantiate(fdbkGoodcoin);
            }
            else
            {
                observedFeedbackcoin = Instantiate(fdbkSpecialcoin);
            }
            observedFeedbackcoin.transform.position = obscoinpos;
            observedFeedbackcoin.GetComponent<Collider>().enabled = false;
            FeedbackCoin thisfcoin = observedFeedbackcoin.GetComponent<FeedbackCoin>();
            if (thisfcoin)
                thisfcoin.coinValue = observedCoinValue;
            else
                Debug.Log("<color=red>[TaskMan] Very weird: feedback coin is missing FeedbackCoin script, probably wrong prefab assigned in the scene</color>");
        }
    }
    void FinishPindropWatching(bool Voted=false) {
        //means other participant has learned all locations, so end the watchingpindropping
        Logger.Loginfo(DataType: "Event", LogMessage: "Finished watching other participant's pindropping.");
        uitext.text = finishWatchPinDroppingMsg;
        mode = socialModes.connectedIdle;
        trailObject.gameObject.SetActive(false);
        
        otherSwapvoted = false;

        if(Voted){
            ObsRewardComm.HeadsetComm.UpdateStatus(ObsRewardComm.TerminalCmds.T_VotedSwap, "");
        }
    }
    void EnterRepositioning(){
        
        ready2startBlock = false;
        otherReady2startBlock = false;
        uitext.text = positionMsg;
        
        mode = socialModes.repositioning;

        initialposIndicator.gameObject.SetActive(true);
        position2startBlock = new Vector3(pos2startblockx, pos2startblocky, pos2startblockz);
    }
    public void SetTaskBlock_i(int BlockNum) {
        taskBlock_i = BlockNum;
    }
    public void SetRecoverValues(int RecRound, int RecPerfect, float RecRew) {
        rec_dropround = RecRound;
        rec_perfectruns = RecPerfect;
        rec_reward = RecRew;
    }
    public void SetAcumulatedReward(float AcumulatedReward){
        totalReward = AcumulatedReward;
    }
}
