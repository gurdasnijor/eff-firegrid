namespace Eff.Foundation

open Eff

type KvEvent<'key, 'value> =
    | Put of key: 'key * value: 'value
    | Delete of key: 'key

type KvStore<'key, 'value when 'key: comparison> =
    private
        { Basin: S2.Basin
          Codec: SubjectHistory.Codec<KvEvent<'key, 'value>>
          Subject: SubjectHistory.SubjectId
          View: StateView<KvEvent<'key, 'value>, Map<'key, 'value>> }

module KvStore =
    let private apply state (record: SubjectHistory.StoredRecord<KvEvent<'key, 'value>>) =
        match record.Body with
        | Put(key, value) -> state |> Map.add key value
        | Delete key -> state |> Map.remove key

    let start basin codec subject recoverFrom =
        async {
            let! view = StateView.start basin codec subject recoverFrom Map.empty apply

            return
                { Basin = basin
                  Codec = codec
                  Subject = subject
                  View = view }
        }

    let put key value store =
        SubjectHistory.append store.Basin store.Codec store.Subject [ Put(key, value) ]

    let delete key store =
        SubjectHistory.append store.Basin store.Codec store.Subject [ Delete key ]

    let get consistency key store =
        async {
            let! view = StateView.read consistency store.View
            return view.AppliedTail, view.State |> Map.tryFind key
        }

    let stop store = StateView.stop store.View
