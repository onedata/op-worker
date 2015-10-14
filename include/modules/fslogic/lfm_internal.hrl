%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Internel header for logical_file_manager
%%% @end
%%%-------------------------------------------------------------------
-author("Rafal Slota").


%% Internal opaque file-handle used by logical_file_manager
-record(lfm_handle, {
    sfm_handles = #{} :: #{term() => {term(), storage_file_manager:handle()}},
    fslogic_ctx :: fslogic_worker:ctx(),
    file_uuid :: file_meta:uuid(),
    open_type :: rw | rd | rdwr
}).