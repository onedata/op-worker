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

-type sfm_handles_map() :: #{term() => {term(), storage_file_manager:handle()}}.

%% Internal opaque file-handle used by logical_file_manager
-record(lfm_handle, {
    file_location :: file_location:model_record(),
    provider_id :: oneprovider:id(),
    sfm_handles = #{} :: sfm_handles_map(),
    fslogic_ctx :: fslogic_worker:ctx(),
    file_uuid :: file_meta:uuid(),
    open_mode :: helpers:open_mode()
}).