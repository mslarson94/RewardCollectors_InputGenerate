% Martin Seeber, 2025
% Suthana Lab, UCLA

clearvars
close all

folder = [pwd filesep 'Data'];

Fs = 250;
f_line = 60;
f_tm = 62.5;

% voltage scale in uV (for high gain)
V_scale = 800/1024;   

% define frequency axis
f_axis = 2.^[0:0.1:7];
f_axis = f_axis(f_axis<90);
N_f = length(f_axis);

iEEG_runs = concat_RNS(folder);
iEEG = iEEG_runs{1}.' * V_scale;

[N, N_chan] = size(iEEG);

% notch filter line
d_line  = fdesign.notch('N,F0,BW', 4, f_line*2/Fs, 2/Fs);
Hd_line = design(d_line);
[b_line, a_line] = sos2tf(Hd_line.sosMatrix);

% notch filter telemetry
d_tm  = fdesign.notch('N,F0,BW', 4, f_tm*2/Fs, 2/Fs);
Hd_tm = design(d_tm);
[b_tm, a_tm] = sos2tf(Hd_tm.sosMatrix);

% bandpass 15-80 Hz for IED signal detection
[b_IED, a_IED] = butter(4, [15, 80]*2/Fs, 'bandpass');

iIED = filtfilt(b_IED, a_IED, iEEG);

H = abs(hilbert(iEEG));
H_IED = abs(hilbert(iIED));

ix_IED_tsh = false(size(iEEG));
ix_IED = false(size(iEEG));
IED_margin = 64;

% detect IED perios using thresholds
for chan = 1:N_chan
    
    tsh_H = median(H(:, chan));
    tsh_H_IED = median(H_IED(:, chan));
    
    ix_IED_tsh(:,chan) = or((H(:,chan) > 5*tsh_H), (H_IED(:,chan) > 5*tsh_H_IED));
    ix_IED(:,chan) = movmean(ix_IED_tsh(:,chan), IED_margin) > 0; 
end

% TF decomposition using Morse wavelets
TF = zeros(N,N_chan,N_f);

for chan = 1:N_chan
    [F, fb] = cwt(iEEG(:,chan),Fs,'FrequencyLimits',[min(f_axis),max(f_axis)]);
    TF(:,chan,:) = flipud(abs(F)).';
end

time = 0:1/Fs:(N-1)/Fs;

figure; 
subplot(2,1,1)
surf( f_axis, time, squeeze(TF(:,1,:))); shading interp
view(90,270)
ylabel(['time [s]'])
xlabel(['f [Hz]'])
set(gca, 'XScale', 'log');
xticks([1,2,4,8,16,32,64])
xlim([min(f_axis),max(f_axis)])
ylim([min(time),max(time)])
set(gca,'fontsize',18);

% interpolate IED periods to avoid discontinuities
for chan = 1:N_chan

    id_IED_tmp = find(ix_IED(:,chan));
    TF_interp = interp1(find(~ix_IED(:,chan)), squeeze(TF(~ix_IED(:,chan),chan,:)), id_IED_tmp );
    TF(id_IED_tmp(~isnan(TF_interp(:,1)),1), chan,:) = TF_interp(~isnan(TF_interp(:,1)),:);
end

subplot(2,1,2)
surf( f_axis, time, squeeze(TF(:,1,:))); shading interp
view(90,270)
ylabel(['time [s]'])
xlabel(['f [Hz]'])
set(gca, 'XScale', 'log');
xticks([1,2,4,8,16,32,64])
xlim([min(f_axis),max(f_axis)])
ylim([min(time),max(time)])
set(gca,'fontsize',18);
