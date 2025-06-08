% Suthana Lab, UCLA
% adapted by Martin Seeber, 2022

function Ephys = RNS_identify_mrk(Ephys, txtFile)

mark_temp1 = [0 -1 -1 0 0 -1 -1 0 0 0 0 0 0 -1 -1 0];
mark_temp2 = [0 -1 -1 0 0 0 0 0 0 -1 -1 0];

% mark_temp1 = [0 -1 -1 0 0 0 0 0 0  -1 -1 0];
% mark_temp2 = [0 -1 -1 0 0 0 0 0 0 0 0 0 0  -1 -1 0];
% mark_temp3 = [0 -1 -1 0 0 -1 -1 0];
% mark_temp4 = [0 -1 -1 0 ];

single_chan = Ephys.raw(1,:); % {i_trial, i_channel};
N = size(single_chan,2);

[mrk_corr1, ~] = xcorr(single_chan, mark_temp1);
mrk_corr1 = mrk_corr1(N:end);

[mrk_corr2, ~] = xcorr(single_chan, mark_temp2);
mrk_corr2 = mrk_corr2(N:end);
% ptrn2 starts 4 samples later
mrk_corr2 = circshift(mrk_corr2,-4);

I = or(mrk_corr1 > 0.9 * max(mrk_corr1), mrk_corr2 > 0.9 * max(mrk_corr2));

I = [0, diff(I)==-1];
idx = find(I);

% refine marker detection
val = zeros(sum(I)-1,1);

for cnt = 1:sum(I)-1
     val(cnt) = sum( (single_chan(idx(cnt)-1:idx(cnt)+size(mark_temp1,2)-2) == -512 ) == (mark_temp1==-1));
end

idx = idx(val>=10);
I = false(size(I,2),1);
I(idx) = -1;

timestamps = read_logfile(txtFile);
timestamps = reshape(timestamps,2,[])';

id_RNS = contains(timestamps(:,1),'.156');
date_time = datetime(timestamps(id_RNS,2))  + hours(7);
RNS_stamp = seconds( date_time - date_time(1) );
RNS_stamp = RNS_stamp - RNS_stamp(1);
N_stamp = size(RNS_stamp,1);

mrk_det = (idx-1).'/Ephys.Fs;
n_shift = (mrk_det + RNS_stamp(end)) < N/Ephys.Fs;

for cnt = 1:sum(n_shift)
    
    ptrn_det(:,cnt) =  mrk_det(cnt) + RNS_stamp; 
    [err(:,cnt), ix_det(:,cnt)] = min(abs(mrk_det - ptrn_det(:,cnt)')) ;
end

[~, id] = min(mean(abs(err)));
max_err = max(abs(err(:,id)));

disp(['pattern detection: ' num2str(round(max_err*1e3)/1e3) ' seconds max shift'])

Tsh_delay = 5;

valid_mrk = err(:,id) < Tsh_delay ; 
Mrk = (mrk_det(ix_det(valid_mrk,id)));

if any(err(:,id) >= Tsh_delay )

    warning([ num2str(sum(valid_mrk ==0)) ' markers were not detected'])
    disp('predicting missing markers...')
    [P, S] = polyfit( ptrn_det(valid_mrk,id), Mrk,1);
    [Mrk_mdl, delta] = polyval(P, ptrn_det(:,id), S);
    disp(['prediction error std: ' num2str(max(delta)*1e3) ' ms'])
    Mrk(~valid_mrk,1) = Mrk_mdl(~valid_mrk,1);
end


Ephys.mrk = round(Mrk*Ephys.Fs) + 1;

time = ([1:N]-1)/Ephys.Fs;
Ephys.daytime = date_time(1) + seconds(time-ptrn_det(1,id));

figure;
plot(Ephys.daytime, single_chan, 'r')
hold on
plot(Ephys.daytime, I*(-512), 'b')
plot(Ephys.daytime(1) + seconds(ptrn_det(:,id)), ones(N_stamp,1)*-512, 'ro')
plot(Ephys.daytime(1) + seconds((Ephys.mrk-1)/Ephys.Fs), ones(N_stamp,1)*-512, 'bo' )
% if any(err(:,id) >= Tsh_delay )
%     plot(Ephys.daytime(1) + seconds(Mrk_mdl), ones(N_stamp,1)*-512, 'mo')
%     plot(Ephys.daytime(1) + seconds(Mrk_mdl), ones(N_stamp,1)*-512, 'mx')
% end
xlim([min(Ephys.daytime) max(Ephys.daytime)])
set(gcf,'Position',[0,20,1800,500])
set(gca,'Fontsize',12)
xlabel('day time')
ylabel('iEEG')

I  = false(1,N);
I(Ephys.mrk) = true;

% I = ptrn_corr > 0.75 * (max(abs(ptrn_corr))) | ptrn_corr < 0.9*min(ptrn_corr);

%         I1 = a1 > 0.9 * (max(abs(a1)));
%         I1 = logical([I1(5:end) zeros(1,4)]);
%         I2 = a2 > 0.9 * (max(abs(a2)));
%         I3 = a3 > 0.9 * (max(abs(a3)));
%         I4 = a4 > 0.9 * (max(abs(a4)));

J = logical([0 I(1:end-1)]);
mark_ind = find(J == 1);
for i_mark = 1 : sum(J)
    J(mark_ind(i_mark) - 2:mark_ind(i_mark) + size(mark_temp1,2)-3) = -1 * mark_temp1;
end
valid_samples = find(J == 0);
lfp_valid = Ephys.raw(:,valid_samples);
all_samples = 1:N;
lfp_new = spline(valid_samples, lfp_valid, all_samples);

%%
% plot(time,lfp_new(1,:),'m')

%% update struct
Ephys.raw = lfp_new;

end
