mark_temp = [0 -1 -1 0 0 -1 -1 0 0 0 0 0 0 -1 -1 0];
mark_temp1 = [0 -1 -1 0 0 0 0 0 0  -1 -1 0];
mark_temp2 = [0 -1 -1 0 0 0 0 0 0 0 0 0 0  -1 -1 0];
mark_temp3 = [0 -1 -1 0 0 -1 -1 0];
mark_temp4 = [0 -1 -1 0 ];

fileList = dir("./");
Fs = 250;
n_channels = 4;

for i = 1 : length(fileList)
    fileName = fileList(i).name;
    if(length(fileName) > 6)
        if(fileName(end-3:end) == ".mat")
            data = load(fileName);
            data = double(data.data);
            data_c = zeros(4, length(data));
            for i_channel = 1 : n_channels
                test_data_real = data(i_channel, :);
                time_real = 0 : length(test_data_real) - 1;
                time_real = time_real / Fs;
                [a,l] = xcorr(test_data_real, mark_temp);
                a = a(length(test_data_real):end);
                [a1,l1] = xcorr(test_data_real, mark_temp1);
                [a2,l2] = xcorr(test_data_real, mark_temp2);
                [a3,l3] = xcorr(test_data_real, mark_temp3);
                [a4,l4] = xcorr(test_data_real, mark_temp4);
                a1 = a1(length(test_data_real):end);
                a2 = a2(length(test_data_real):end);
                a3 = a3(length(test_data_real):end);
                a4 = a4(length(test_data_real):end);
            
                I = a > 0.9 * (max(abs(a)));
                
                J = logical([0 I(1:end-1)]);
                J_orig = J;
            
                i_orig = 1;
                pulse_duration = 1;
                while i_orig < length(J_orig)
                    pulse_duration = 1;
                    if(J_orig(i_orig) == 1)
                        while(J(i_orig + pulse_duration) == 1)
                            pulse_duration = pulse_duration + 1; 
                        end
                        if pulse_duration > 1
                            J_orig(i_orig:i_orig+pulse_duration) = 0;
                        end
                    end
                    i_orig = i_orig+pulse_duration;
                end
            
                i_orig = 1;
                sig1 = (data(i_channel,:) == -512);
                for i_orig = 1:length(J_orig)
                    if(J_orig(i_orig) == 1)
                        if(sum(sig1(i_orig-1:i_orig+14) + mark_temp) ~= 0)
                            J_orig(i_orig) = 0;
                        end
                    end
                end
   
                mark_ind = find(J_orig == 1);
                for i_mark = 1 : sum(J_orig)
                    J_orig(mark_ind(i_mark) + 1:mark_ind(i_mark) + 14) = -1 * mark_temp(3:end);
                end
            
                valid_samples = find(J_orig == 0);
                b = test_data_real(valid_samples);
                all_samples = 1:1:length(test_data_real);
                lfp_new = spline(single(valid_samples), b, all_samples);
                data_c(i_channel,:) = lfp_new;
            end
            
            marks = mark_ind;
            time = 0:length(data_c)-1;
            time = time/250;
            save(strcat(fileName(1:end-4), "_no_mark.mat"), 'data_c', 'time', 'marks')
        end
    end

end