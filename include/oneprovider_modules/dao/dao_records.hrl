%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: List of supported (by DAO) records
%% @end
%% ===================================================================
-author("Rafal Slota").


-ifndef(DAO_RECORDS_HRL).
-define(DAO_RECORDS_HRL, 1).

-include_lib("dao/include/common.hrl").
-include_lib("ctool/include/global_registry/gr_openid.hrl").
-include_lib("ctool/include/global_registry/gr_groups.hrl").
-include("oneprovider_modules/dao/dao_spaces.hrl").

%% Every record that will be saved to DB have to be "registered" with this define.
%% Each registered record should be listed in defined below 'case' block as fallow:
%% record_name -> ?record_info_gen(record_name);
%% where 'record_name' is the name of the record. 'some_record' is an example.
-define(dao_record_info(R),
    case R of
        some_record         -> ?record_info_gen(some_record);
        cm_state            -> ?record_info_gen(cm_state);
        host_state          -> ?record_info_gen(host_state);
        node_state          -> ?record_info_gen(node_state);
        file                -> ?record_info_gen(file);
        file_location       -> ?record_info_gen(file_location);
        file_block          -> ?record_info_gen(file_block);
        file_descriptor     -> ?record_info_gen(file_descriptor);
        file_meta           -> ?record_info_gen(file_meta);
        file_lock           -> ?record_info_gen(file_lock);
        available_blocks    -> ?record_info_gen(available_blocks);
        storage_info        -> ?record_info_gen(storage_info);
        user                -> ?record_info_gen(user);
        share_desc          -> ?record_info_gen(share_desc);
        fuse_session        -> ?record_info_gen(fuse_session);
        connection_info     -> ?record_info_gen(connection_info);
        quota               -> ?record_info_gen(quota);
        session_cookie      -> ?record_info_gen(session_cookie);
        fuse_group_info     -> ?record_info_gen(fuse_group_info);
        storage_helper_info -> ?record_info_gen(storage_helper_info);
        space_info          -> ?record_info_gen(space_info);
        id_token_login      -> ?record_info_gen(id_token_login);
        dbsync_state        -> ?record_info_gen(dbsync_state);
        group_details       -> ?record_info_gen(group_details);
        file_attr_watcher   -> ?record_info_gen(file_attr_watcher);
    %next_record        -> ?record_info_gen(next_record);
        _ -> {error, unsupported_record}
    end).

-endif.
