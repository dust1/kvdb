use crate::error::Result;
use crate::storage::memory::Memory;
use crate::storage::range::Range;
use crate::storage::Store;

struct Test {
    key: &'static [u8],
    value: &'static [u8],
    range: Option<Range>
}

#[test]
fn test_memory_kv() -> Result<()> {
    let tests = [
        Test {
            key:"a".as_bytes(),
            value: "aaaaa".as_bytes(),
            range: None
        },
        Test {
            key: "b".as_bytes(),
            value: "bbbbbbb".as_bytes(),
            range: None
        },
        Test {
            key: "张三".as_bytes(),
            value: "李四".as_bytes(),
            range: None
        },
        Test {
            key: "c".as_bytes(),
            value: "ccc".as_bytes(),
            range: Some(Range::from("a".as_bytes().to_vec()..="c".as_bytes().to_vec()))
        }

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
            store.scan(range);
        }
    }

    Ok(())
}