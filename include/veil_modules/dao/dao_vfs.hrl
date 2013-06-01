%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: dao_vfs header
%% @end
%% ===================================================================


-record(file_location, {storage_helper_id = "", file_id = ""}).
-record(file_lock, {type = 1, uid = "", sid = "", pid = 0, offset = 0, size = 0}).
-record(file, {type = 1, name = "", size = 0, uid = "", gids = [], perms = 0, parent = "", ref_file = "", location = #file_location{}, locks = []}).

-record(file_tag, {key = "", value = []}).
-record(file_meta, {file = "", tags = [], mime_type = "", last_modified = 0, description = ""}).

-record(file_descriptor, {file = "", mode = 0, uid = "", sid = "", pid = 0, cursor = 0}).