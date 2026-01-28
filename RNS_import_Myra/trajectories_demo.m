% Martin Seeber, 2025
% Duke University

close all 
clearvars

Fs = 250;

data = load('walks_demo.mat');

% determine average sample number for walks
N_left = round(mean(cellfun(@length,data.walks.left)));
N_right = round(mean(cellfun(@length,data.walks.right)));

time_bin = 0.2;

% intial alignment - time warping

figure;

warped_left = zeros(N_left,2,size(data.walks.left,1));

for walk = 1:size(data.walks.left,1)
     
    N_walk = size(data.walks.left{walk},1);

    xq = linspace(1,N_walk, N_left);
    warped_left(:,:,walk) = interp1(1:N_walk, data.walks.left{walk}, xq );
    subplot(1,3,1)
    plot(data.walks.left{walk})
    set(gca,'fontsize',18);
    xlabel('#samples')
    ylabel('position [m]')
    title('original data')
    hold on
    subplot(1,3,2)
    plot(warped_left(:,:,walk))
    set(gca,'fontsize',18);
        xlabel('#samples')
    ylabel('position [m]')
    title('linear warped')
    hold on
end

set(gcf,'Position',[0,250,1250,350])

walk_left_avg = mean(warped_left,3);

% refine alignment with dynamic time warping

bin_left = [0:time_bin*Fs:N_left, N_left];
walks_left = zeros(N_left,2,size(data.walks.left,1));

for walk = 1:size(data.walks.left,1)

    N_walk = size(data.walks.left{walk},1);
    
    [~,ix,iy] = dtw( data.walks.left{walk}.', walk_left_avg', Fs );

    id = ismember(iy, bin_left);
    idx = [0;find([0;diff(id)==1])];
    
    xi = zeros(N_left,1);
    
    for bin = 1:size(bin_left,2)-1
        
        xi(bin_left(bin)+1:bin_left(bin+1),1) = linspace( ix(idx(bin)+1), ix(idx(bin+1)), bin_left(bin+1)-bin_left(bin) );
    end
    
    % warping the original data on average trajectory
    % same warping would work for neural data by replacing
    % data.walks.left{walk} with synchronized neural data with length N_walk 
    walks_left(:,:,walk)  = interp1( 1:N_walk, data.walks.left{walk}, xi ); 
    
    subplot(1,3,3)
    plot(walks_left(:,:,walk))
    set(gca,'fontsize',18);
    xlabel('#samples')
    ylabel('position [m]')
    title('dynamic warped')
    hold on
end

