% synch demo for two samples start (assuming constant drift)

% generate signals for demo

Fs = 250;

[b, a] = butter(4, [4,8]*2/Fs, 'bandpass');
RNS = filtfilt(b, a, randn(20*Fs,1));

[d, c] = butter(2, [0.2,1]*2/Fs, 'bandpass');
behav = filtfilt(d, c, randn(19.8*Fs,1));


figure; 
subplot(2,1,1)
plot(RNS)
subplot(2,1,2)
plot(behav)
xlim([0,20*Fs])

% alignment, create query points for the behavioral signals, 
% with N_RNS % point but covering the range [1:N_behav] 

N_RNS = size(RNS,1);
N_behav = size(behav,1);

xi = linspace(1,N_behav, N_RNS);
figure; plot(xi);

behav_synch = interp1(1:N_behav,behav, xi );
