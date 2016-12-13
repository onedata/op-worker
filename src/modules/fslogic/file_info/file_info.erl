%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Opaque type storing informations about file.
%%% @end
%%%--------------------------------------------------------------------
-module(file_info).
-author("Tomasz Lichon").

-type attribute() :: guid | uuid | path | share_id.
-type file_info() :: #{}.

%% API
-export([get_share_id/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Get file's share_id
%% @end
%%--------------------------------------------------------------------
-spec get_share_id(fslogic_context:ctx()) -> od_share:id() | undefined.
get_share_id(Ctx) -> %todo TL return share_id
    fslogic_context:get_share_id(Ctx).


%%%===================================================================
%%% Internal functions
%%%===================================================================