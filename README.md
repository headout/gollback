# gollback
[![go reference](https://pkg.go.dev/badge/github.com/abhikvarma/gollback.svg)](https://pkg.go.dev/github.com/abhikvarma/gollback#section-documentation) [![tests](https://github.com/abhikvarma/gollback/actions/workflows/test.yml/badge.svg)](https://github.com/abhikvarma/gollback/actions/workflows/test.yml)

a simple [pgx](https://github.com/jackc/pgx) transaction manager that automatically rolls back on error.  
supports timeouts and read-only transactions.

## usage

NewTxnProvider returns a transaction manager and a function to get a connection from the pool.
```go
func main() {
    ctx := context.Background()
    
    pool, err := pgxpool.New(ctx, "postgres://user:pass@localhost:5432/db")
    if err != nil {
        panic(err)
    }
    defer pool.Close()
    
    txnManager, getConn := gollback.NewTxnProvider(pool)
}                                                                                                                                                                                                                                                                                                              
```  
<br>

replace `pgxpool.Pool` in your repositories with `getConn`
```go
type UserRepo struct {
    db getConn
}

func (r *UserRepo) GetByID(ctx context.Contex, id int) (*User, error) {
    return r.db.QueryRow(ctx, "SELECT * FROM users WHERE id = $1", id)
}
```
<br>

your services can now use the transaction manager to string together multiple repository calls into a single transaction.
```go
type Service struct {
    userRepo UserRepo
    postRepo PostRepo
    txnManager gollback.TxnManager
}

func (s *Service) CreatePost(ctx context.Context, userID int, post *Post) error {
    return s.txnManager.Do(ctx, func(ctx context.Context) error {
        _, err := s.userRepo.GetByID(ctx, UserID)
        if err != nil {
            return err
        }
        _, err = s.postRepo.Create(ctx, post, user.Name)
        if err != nil {
            return err
        }
        return nil
    }
```
<br>

* the transaction is rolled back if the function returns an error
* the transaction is committed if the function returns nil

## advanced

### using timeouts
```go
s.txnManager.Do(ctx, func(ctx context.Context) error {
    return s.userRepo.GetByID(ctx, 123)
}, gollback.WithTimeout(5*time.Second))
```

### read-only transactions
```go
s.txnManager.Do(ctx, func(ctx context.Context) error {
    return s.userRepo.GetByID(ctx, 123)
}, gollback.ReadOnly())
```
the above functional options can be combined.

### error handling
the following typed errors are returned by the transaction manager:
* `gollback.TxnBeginError`
* `gollback.TxnCommitError`
* `gollback.TxnRollbackError`
  * the original error is available via `.Cause`