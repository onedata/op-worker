%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% It is the behaviour of synchronizer callback module.
%%% It defines ways of handling files synchronization by replica_synchronizer 
%%% depending on calling module.
%%% @end
%%%--------------------------------------------------------------------
-module(synchronizer_callback_behaviour).
-author("Michal Stanisz").

%% API

-callback flush_stats(od_space:id(), transfer:id(), #{od_provider:id() => integer()}) -> 
    ok | {error, term()}.

