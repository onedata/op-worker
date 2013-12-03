%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: dao_cluster header
%% @end
%% ===================================================================

%% This record contains environmental variables send by FUSE client
%% Variables are stored in 'vars' list. Entry format: {Name :: atom(), Value :: string()}
-record(fuse_env, {uid, hostname = "", vars = []}).