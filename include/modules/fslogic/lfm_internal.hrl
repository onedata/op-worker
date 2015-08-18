%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc @todo: Write me!
%%% @end
%%%-------------------------------------------------------------------
-author("Rafal Slota").


-record(lfm_handle, {
    sfm_handles :: storage_file_manager:handle(),
    fslogic_ctx :: fslogic_worker:ctx(),
    file_uuid :: file_meta:uuid(),
    open_type
}).