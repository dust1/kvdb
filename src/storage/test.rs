use crate::error::Result;
use crate::storage::memory::Memory;
use crate::storage::range::Range;
use crate::storage::Store;

struct Test {
    key: &'static [u8],
    value: &'static [u8],
    range: Option<Range>,
    range_value: Vec<(Vec<u8>, Vec<u8>)>,
}

#[test]
fn test_memory_kv() -> Result<()> {
    let tests = [
        Test {
            key: b"a",
            value: b"aaaaa",
            range: None,
            range_value: vec![],
        },
        Test {
            key: b"b",
            value: b"bbbbbbb",
            range: None,
            range_value: vec![],
        },
        Test {
            key: "张三".as_bytes(),
            value: "李四".as_bytes(),
            range: None,
            range_value: vec![],
        },
        Test {
            key: b"c",
            value: b"ccc",
            range: Some(Range::from(b"a".to_vec()..b"c".to_vec())),
            range_value: vec![
                (b"b".to_vec(), b"bbbbbbb".to_vec()),
                (b"a".to_vec(), b"aaaaa".to_vec()),
            ],
        },
    ];
    let mut store: Box<dyn Store> = Box::new(Memory::new());

    for test in tests {
        let key = test.key;
        let value = test.value;
        store.set(key, value.to_vec())?;

        if let Some(v) = store.get(key)? {
            assert_eq!(v, value.to_vec());
        } else {
            assert!(false);
        }

        if let Some(range) = test.range {
            let scan = store.scan(range).rev().collect::<Result<Vec<_>>>()?;
            assert_eq!(test.range_value, scan);
        }
    }

    Ok(())
}
