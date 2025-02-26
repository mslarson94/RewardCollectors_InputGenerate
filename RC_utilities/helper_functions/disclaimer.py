
def disclaimer():
    '''
    Helper function that returns a disclaimer text string. 
    '''
    disclaimer ="""
    ********************* FORMULAS *************************************
    
    d' =  Z(Hit Rate)  - Z(False Alarm Rate)
    LDI = Lure % Correct Rejection - Target % Miss
    criterion = (Z(Hit Rate ) + Z( False Alarm Rate )) / 2

    - For these calculations we use Z from the SciPy Stats Norm module
        for the inverse cumulative distribution function (otherwise
        known as the quantile function or the percent-point function).
    - Hit Rate refers to Target Hit Rate
    - False Alarm Rate refers to Foil False Alarm rate or each Lure
        bin False Alarm Rate (Foil FA, High Lure FA, Low Lure FA).
    - When Hit Rate or False Alarm rate are 0 or 1, we employ the
        the method as outlined in Macmillan & Kaplan's 1985 paper:
        Detection theory analysis ofgroup data: "Estimating sensitivity
        from average hit and false-alarm rates"
            - Rates of 0 are replaced with 0.5/n
            - Rates of 1 are replaced with (n - 0.5)/n
        where n is the number of signal or noise trials

    Formulas drawn from Stanislaw's 1999 article:
    'CALCULATION OF SIGNAL DETECTION THEORY MEASURES'

    ******************** MEANING OF VALUES *****************************
    
    Stimuli by Task Type:
    
          Task Type | Object  | Spatial    | Temporal
          
          Target    | Target  | Same       | Adj
          Hi Lure   | LureH   | Small Mv   | Eight
          Lo Lure   | LureL   | Large Mv   | Sixteen
          Foil      | Foil    | Corner Mv  | PR

        LureH = High Similarity Lure
        LureL = Low Similarity Lure
        Mv    = Move
        Adj   = Adjacent
        PR    = Primacy/Recency
        
    d' 
        - measures the distance between the signal (Target
            Stimuli) & noise (Foil & Lure Stimuli)  means in
            standard deviation units. Unaffected by response bias
            (Provided that signal & noise distributions are normal,
            and signal & noise distributions have the same
            distributions -> cannot be assumed by 'Yes'/'No' tasks
            but covered by Rating Tasks). 
        - Value of 0 indicates inability to distinguish signals
            from noise 
        - Positive values  correspond to greater ability to
            distinguish signals from noise. 
        - Negative values indicate sampling error or response
            confusion (responding 'yes' when intending to respond
            'no')
    criterion
        - measures response bias
        - Negative Values indicate bias towards responding 'no'
        - Positive Values indicate bias towards responding 'yes'
        - Values of 0 indicate complete lack of response bias. 
    LDI
        - measures discrimination performance of discriminating
            lure items (or locations) from target.
        - Greater values indicate better discrimination
            performance
    
    ********************* DISCLAIMER ***********************************
        
    These analyses are ***not*** quality checked by this script.
    YOU, the experimenter, will need to go through this file and
    determine whether and how you will be excluding subjects from
    your analyses. Some suggestions:
        - A negative d' Foil value indicates sampling error or
          response confusion (responding 'New' when intending to
          respond 'Old') at the easiest level of discrimination
          (Target from Novel Foil)
        - A d' Foil value of 0 indicates inability to distinguish
          signals from noise at the easiest level of discrimination
          (Target from Novel Foil)
        - Values of -1 or 1 in criterion Foil indicate complete bias
          toward responding 'New' or 'Old' (respectively to both
          Target & Foil stimuli.
        - Additionally, one could set a total test response rate
          threshold (for example: exclude less response rates of
          less than 75% of all presented stimuli)
    An additional note:
        For all Ratcliff-Diffusion model measures, you ***must***
        determine whether the subject's fingers were given clear
        instructions to keep their fingers on their response keys
        throughout the entire task. For all tasks administered on
        version 1.1 or later, you are guaranteed to have those
        instructions + practice tests administered as a part of the
        task. For all tasks administered on version 1.0, see
        your experiment documentation for further information.
        
    ********************************************************************
    """
    return disclaimer 
